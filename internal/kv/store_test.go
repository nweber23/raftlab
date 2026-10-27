package kv_test

import (
	"encoding/json"
	"testing"

	"github.com/nweber23/raftlab/internal/kv"
)

func apply(s *kv.Store, op kv.Op) {
	b, _ := json.Marshal(op)
	s.Apply(1, b)
}

func TestApplySetGetDelete(t *testing.T) {
	s := kv.New()
	apply(s, kv.Op{Kind: kv.OpSet, Key: "a", Value: "1", Client: "c", Seq: 1})
	apply(s, kv.Op{Kind: kv.OpSet, Key: "b", Value: "2", Client: "c", Seq: 2})
	apply(s, kv.Op{Kind: kv.OpDelete, Key: "a", Client: "c", Seq: 3})
	d := s.Data()
	if len(d) != 1 || d["b"] != "2" {
		t.Fatalf("data %v", d)
	}
}

func TestDuplicateRequestAppliedOnce(t *testing.T) {
	s := kv.New()
	apply(s, kv.Op{Kind: kv.OpSet, Key: "k", Value: "old", Client: "c", Seq: 1})
	apply(s, kv.Op{Kind: kv.OpSet, Key: "k", Value: "new", Client: "c", Seq: 2})
	// A delayed duplicate of request 1 must not roll the key back.
	apply(s, kv.Op{Kind: kv.OpSet, Key: "k", Value: "old", Client: "c", Seq: 1})
	apply(s, kv.Op{Kind: kv.OpSet, Key: "k", Value: "new", Client: "c", Seq: 2})
	if got := s.Data()["k"]; got != "new" {
		t.Fatalf("k = %q", got)
	}
}

func TestClientsAreIndependent(t *testing.T) {
	s := kv.New()
	apply(s, kv.Op{Kind: kv.OpSet, Key: "x", Value: "1", Client: "a", Seq: 5})
	apply(s, kv.Op{Kind: kv.OpSet, Key: "x", Value: "2", Client: "b", Seq: 1})
	if s.Data()["x"] != "2" {
		t.Fatal("client b's first request was treated as a duplicate")
	}
}

func TestSnapshotRestoreKeepsDataAndSessions(t *testing.T) {
	s := kv.New()
	apply(s, kv.Op{Kind: kv.OpSet, Key: "k", Value: "v", Client: "c", Seq: 1})
	img := s.Snapshot()

	r := kv.New()
	r.Restore(img)
	apply(r, kv.Op{Kind: kv.OpSet, Key: "k", Value: "replayed", Client: "c", Seq: 1})
	if r.Data()["k"] != "v" {
		t.Fatalf("session table lost in snapshot: %v", r.Data())
	}
	apply(r, kv.Op{Kind: kv.OpSet, Key: "k", Value: "next", Client: "c", Seq: 2})
	if r.Data()["k"] != "next" {
		t.Fatal("restored store rejects new requests")
	}
}

func TestMalformedCommandIsIgnored(t *testing.T) {
	s := kv.New()
	s.Apply(1, []byte("not json"))
	if len(s.Data()) != 0 {
		t.Fatal("garbage changed state")
	}
}
