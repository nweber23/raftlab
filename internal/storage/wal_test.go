package storage

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nweber23/raftlab/internal/raft"
)

func e(term uint64, cmd string) raft.Entry { return raft.Entry{Term: term, Command: []byte(cmd)} }

func mustOpen(t *testing.T, dir string) (*WAL, raft.PersistentState) {
	t.Helper()
	w, st, err := Open(dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return w, st
}

func TestFreshDirIsEmpty(t *testing.T) {
	w, st := mustOpen(t, t.TempDir())
	defer w.Close()
	if st.Term != 0 || st.VotedFor != 0 || len(st.Log) != 0 {
		t.Fatalf("state %+v", st)
	}
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(w.SaveState(1, 2))
	must(w.Append(0, []raft.Entry{e(1, "a"), e(1, "b"), e(1, "c")}))
	must(w.SaveState(3, 0))
	must(w.Append(1, []raft.Entry{e(3, "x")})) // overwrites b, c
	must(w.Close())

	w, st := mustOpen(t, dir)
	defer w.Close()
	want := raft.PersistentState{Term: 3, VotedFor: 0, Log: []raft.Entry{e(1, "a"), e(3, "x")}}
	if !reflect.DeepEqual(st, want) {
		t.Fatalf("got %+v, want %+v", st, want)
	}
}

func TestSnapshotCompactsAndSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.SaveState(2, 1)
	w.Append(0, []raft.Entry{e(1, "a"), e(1, "b"), e(2, "c"), e(2, "d")})
	if err := w.SaveSnapshot(raft.Snapshot{Index: 3, Term: 2, Data: []byte("img")}, []raft.Entry{e(2, "d")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(4, []raft.Entry{e(2, "e")}); err != nil { // keeps working on the new file
		t.Fatal(err)
	}
	w.Close()

	w, st := mustOpen(t, dir)
	defer w.Close()
	want := raft.PersistentState{
		Term: 2, VotedFor: 1, Log: []raft.Entry{e(2, "d"), e(2, "e")},
		SnapIndex: 3, SnapTerm: 2, SnapData: []byte("img"),
	}
	if !reflect.DeepEqual(st, want) {
		t.Fatalf("got %+v, want %+v", st, want)
	}
}

// tornFile writes three entries one record each and returns the file path
// and the size after the first two.
func tornFile(t *testing.T) (dir string, twoRecords int64) {
	dir = t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Append(0, []raft.Entry{e(1, "one")})
	w.Append(1, []raft.Entry{e(1, "two")})
	fi, _ := os.Stat(filepath.Join(dir, "raft.wal"))
	w.Append(2, []raft.Entry{e(1, "three")})
	w.Close()
	return dir, fi.Size()
}

func TestTornTailIsDiscardedAtEveryCutPoint(t *testing.T) {
	dir, two := tornFile(t)
	path := filepath.Join(dir, "raft.wal")
	full, _ := os.ReadFile(path)
	for cut := two; cut < int64(len(full)); cut++ {
		if err := os.WriteFile(path, full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		w, st := mustOpen(t, dir)
		if len(st.Log) != 2 || string(st.Log[1].Command) != "two" {
			t.Fatalf("cut at %d: log %+v", cut, st.Log)
		}
		if fi, _ := os.Stat(path); fi.Size() != two {
			t.Fatalf("cut at %d: file not truncated to %d, is %d", cut, two, fi.Size())
		}
		w.Close()
	}
}

func TestCorruptRecordStopsReplay(t *testing.T) {
	dir, two := tornFile(t)
	path := filepath.Join(dir, "raft.wal")
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0xff // flip bits in the last record's payload
	os.WriteFile(path, b, 0o644)
	w, st := mustOpen(t, dir)
	defer w.Close()
	if len(st.Log) != 2 {
		t.Fatalf("log %+v", st.Log)
	}
	if fi, _ := os.Stat(path); fi.Size() != two {
		t.Fatalf("corrupt tail not dropped: size %d want %d", fi.Size(), two)
	}
}

func TestAppendAfterRecoveryReplaysCleanly(t *testing.T) {
	dir, two := tornFile(t)
	path := filepath.Join(dir, "raft.wal")
	b, _ := os.ReadFile(path)
	os.WriteFile(path, append(b[:two:two], 0xde, 0xad, 0xbe), 0o644) // garbage tail
	w, _ := mustOpen(t, dir)
	w.Append(2, []raft.Entry{e(2, "fresh")})
	w.Close()
	w, st := mustOpen(t, dir)
	defer w.Close()
	if len(st.Log) != 3 || string(st.Log[2].Command) != "fresh" {
		t.Fatalf("log %+v", st.Log)
	}
}

func TestImpossibleTruncationIsAnError(t *testing.T) {
	dir := t.TempDir()
	w, _ := mustOpen(t, dir)
	w.Append(5, []raft.Entry{e(1, "gap")}) // log is empty, so index 5 does not exist
	w.Close()
	if _, _, err := Open(dir, Options{}); err == nil {
		t.Fatal("want error for checksum-valid but impossible record")
	}
}
