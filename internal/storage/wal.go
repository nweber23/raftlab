// Package storage implements raft.Storage as a write-ahead log.
//
// The log is a single file of records:
//
//	| length u32 | crc32c u32 | type u8 | payload ... |
//
// length covers type+payload and the checksum covers the same bytes. A crash
// can leave a partial or garbled record at the end of the file (a torn
// write); Open stops at the first record that does not verify and truncates
// the file there, so everything that was fsynced survives and nothing half
// written is ever replayed.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/nweber23/raftlab/internal/raft"
)

const (
	recState    byte = 1 // term u64, votedFor u32
	recAppend   byte = 2 // after u64, count u32, entries (term u64, len u32, bytes)
	recSnapshot byte = 3 // index u64, term u64, data

	headerSize = 8
	maxRecord  = 1 << 30 // sanity bound so a corrupt length cannot trigger a huge allocation
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Options tunes a WAL.
type Options struct {
	// NoSync skips fsync. For simulations that model crashes at process
	// level; never for real deployments.
	NoSync bool
}

// WAL is a raft.Storage backed by a file in one directory.
type WAL struct {
	path string
	opts Options
	f    *os.File

	// Last durable hard state, needed to rewrite the file on compaction.
	term     uint64
	votedFor raft.NodeID
}

var _ raft.Storage = (*WAL)(nil)

// Open opens (creating if needed) the WAL in dir and replays it.
func Open(dir string, opts Options) (*WAL, raft.PersistentState, error) {
	var st raft.PersistentState
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, st, err
	}
	w := &WAL{path: filepath.Join(dir, "raft.wal"), opts: opts}
	data, err := os.ReadFile(w.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, st, err
	}
	st, good, err := replay(data)
	if err != nil {
		return nil, st, fmt.Errorf("storage: %s: %w", w.path, err)
	}
	w.term, w.votedFor = st.Term, st.VotedFor

	if w.f, err = os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0o644); err != nil {
		return nil, st, err
	}
	if int64(good) != int64(len(data)) { // drop the torn tail
		if err := w.f.Truncate(int64(good)); err != nil {
			return nil, st, err
		}
	}
	if _, err := w.f.Seek(int64(good), 0); err != nil {
		return nil, st, err
	}
	if err := w.sync(); err != nil {
		return nil, st, err
	}
	return w, st, nil
}

// replay rebuilds state from data and returns the length of the valid prefix.
// A bad record ends the replay silently; a record that verifies but makes no
// sense (an impossible truncation point) is real corruption and an error.
func replay(data []byte) (raft.PersistentState, int, error) {
	var st raft.PersistentState
	off := 0
	for len(data)-off >= headerSize {
		n := int(binary.LittleEndian.Uint32(data[off:]))
		sum := binary.LittleEndian.Uint32(data[off+4:])
		if n < 1 || n > maxRecord || len(data)-off-headerSize < n {
			break
		}
		body := data[off+headerSize : off+headerSize+n]
		if crc32.Checksum(body, castagnoli) != sum {
			break
		}
		if err := apply(&st, body[0], body[1:]); err != nil {
			return st, off, err
		}
		off += headerSize + n
	}
	return st, off, nil
}

func apply(st *raft.PersistentState, typ byte, p []byte) error {
	bad := func() error { return fmt.Errorf("malformed type %d record", typ) }
	switch typ {
	case recState:
		if len(p) != 12 {
			return bad()
		}
		st.Term = binary.LittleEndian.Uint64(p)
		st.VotedFor = raft.NodeID(binary.LittleEndian.Uint32(p[8:]))
	case recAppend:
		if len(p) < 12 {
			return bad()
		}
		after := binary.LittleEndian.Uint64(p)
		count := int(binary.LittleEndian.Uint32(p[8:]))
		p = p[12:]
		if after < st.SnapIndex || after-st.SnapIndex > uint64(len(st.Log)) {
			return fmt.Errorf("append after %d outside log (%d..%d)", after, st.SnapIndex, st.SnapIndex+uint64(len(st.Log)))
		}
		st.Log = st.Log[:after-st.SnapIndex]
		for i := 0; i < count; i++ {
			if len(p) < 12 {
				return bad()
			}
			term := binary.LittleEndian.Uint64(p)
			l := int(binary.LittleEndian.Uint32(p[8:]))
			p = p[12:]
			if len(p) < l {
				return bad()
			}
			st.Log = append(st.Log, raft.Entry{Term: term, Command: append([]byte(nil), p[:l]...)})
			p = p[l:]
		}
	case recSnapshot:
		if len(p) < 16 {
			return bad()
		}
		st.SnapIndex = binary.LittleEndian.Uint64(p)
		st.SnapTerm = binary.LittleEndian.Uint64(p[8:])
		st.SnapData = append([]byte(nil), p[16:]...)
		st.Log = nil // a snapshot record is always followed by the tail's append record
	default:
		return fmt.Errorf("unknown record type %d", typ)
	}
	return nil
}

func record(typ byte, payload []byte) []byte {
	buf := make([]byte, headerSize+1+len(payload))
	buf[headerSize] = typ
	copy(buf[headerSize+1:], payload)
	binary.LittleEndian.PutUint32(buf, uint32(1+len(payload)))
	binary.LittleEndian.PutUint32(buf[4:], crc32.Checksum(buf[headerSize:], castagnoli))
	return buf
}

func stateRecord(term uint64, votedFor raft.NodeID) []byte {
	p := make([]byte, 12)
	binary.LittleEndian.PutUint64(p, term)
	binary.LittleEndian.PutUint32(p[8:], uint32(votedFor))
	return record(recState, p)
}

func appendRecord(after uint64, entries []raft.Entry) []byte {
	p := make([]byte, 12)
	binary.LittleEndian.PutUint64(p, after)
	binary.LittleEndian.PutUint32(p[8:], uint32(len(entries)))
	for _, e := range entries {
		p = binary.LittleEndian.AppendUint64(p, e.Term)
		p = binary.LittleEndian.AppendUint32(p, uint32(len(e.Command)))
		p = append(p, e.Command...)
	}
	return record(recAppend, p)
}

func snapshotRecord(s raft.Snapshot) []byte {
	p := make([]byte, 16, 16+len(s.Data))
	binary.LittleEndian.PutUint64(p, s.Index)
	binary.LittleEndian.PutUint64(p[8:], s.Term)
	return record(recSnapshot, append(p, s.Data...))
}

func (w *WAL) sync() error {
	if w.opts.NoSync {
		return nil
	}
	return w.f.Sync()
}

func (w *WAL) write(rec []byte) error {
	if _, err := w.f.Write(rec); err != nil {
		return err
	}
	return w.sync()
}

// SaveState implements raft.Storage.
func (w *WAL) SaveState(term uint64, votedFor raft.NodeID) error {
	w.term, w.votedFor = term, votedFor
	return w.write(stateRecord(term, votedFor))
}

// Append implements raft.Storage.
func (w *WAL) Append(after uint64, entries []raft.Entry) error {
	return w.write(appendRecord(after, entries))
}

// SaveSnapshot implements raft.Storage. It compacts the file: a fresh WAL
// holding hard state, the snapshot and the tail is fsynced, then renamed over
// the old one, so a crash leaves either the old or the new file, never a mix.
func (w *WAL) SaveSnapshot(s raft.Snapshot, tail []raft.Entry) error {
	tmp := w.path + ".tmp"
	nf, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	var buf []byte
	buf = append(buf, stateRecord(w.term, w.votedFor)...)
	buf = append(buf, snapshotRecord(s)...)
	buf = append(buf, appendRecord(s.Index, tail)...)
	if _, err := nf.Write(buf); err != nil {
		_ = nf.Close()
		return err
	}
	if !w.opts.NoSync {
		if err := nf.Sync(); err != nil {
			_ = nf.Close()
			return err
		}
	}
	if err := os.Rename(tmp, w.path); err != nil {
		_ = nf.Close()
		return err
	}
	if !w.opts.NoSync {
		if err := syncDir(filepath.Dir(w.path)); err != nil {
			_ = nf.Close()
			return err
		}
	}
	_ = w.f.Close() // the old file is replaced; nothing more to flush
	w.f = nf        // positioned at the end of the new file
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// Close closes the file.
func (w *WAL) Close() error { return w.f.Close() }
