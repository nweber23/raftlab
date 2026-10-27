package transport

import (
	"encoding/json"
	"fmt"

	"github.com/nweber23/raftlab/internal/raft"
)

type envelope struct {
	From raft.NodeID     `json:"from"`
	To   raft.NodeID     `json:"to"`
	Kind string          `json:"kind"`
	Body json.RawMessage `json:"body"`
}

// Encode serializes m as JSON, tagged with its RPC kind.
func Encode(m raft.Message) ([]byte, error) {
	body, err := json.Marshal(m.Body)
	if err != nil {
		return nil, err
	}
	return json.Marshal(envelope{m.From, m.To, m.Kind(), body})
}

// Decode is the inverse of Encode.
func Decode(b []byte) (raft.Message, error) {
	var e envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return raft.Message{}, err
	}
	var body any
	switch e.Kind {
	case "RequestVote":
		body = new(raft.RequestVoteArgs)
	case "RequestVoteReply":
		body = new(raft.RequestVoteReply)
	case "AppendEntries":
		body = new(raft.AppendEntriesArgs)
	case "AppendEntriesReply":
		body = new(raft.AppendEntriesReply)
	case "InstallSnapshot":
		body = new(raft.InstallSnapshotArgs)
	case "InstallSnapshotReply":
		body = new(raft.InstallSnapshotReply)
	default:
		return raft.Message{}, fmt.Errorf("transport: unknown message kind %q", e.Kind)
	}
	if err := json.Unmarshal(e.Body, body); err != nil {
		return raft.Message{}, err
	}
	// Bodies travel by value inside raft.Message.
	switch b := body.(type) {
	case *raft.RequestVoteArgs:
		return raft.Message{From: e.From, To: e.To, Body: *b}, nil
	case *raft.RequestVoteReply:
		return raft.Message{From: e.From, To: e.To, Body: *b}, nil
	case *raft.AppendEntriesArgs:
		return raft.Message{From: e.From, To: e.To, Body: *b}, nil
	case *raft.AppendEntriesReply:
		return raft.Message{From: e.From, To: e.To, Body: *b}, nil
	case *raft.InstallSnapshotArgs:
		return raft.Message{From: e.From, To: e.To, Body: *b}, nil
	default:
		return raft.Message{From: e.From, To: e.To, Body: *body.(*raft.InstallSnapshotReply)}, nil
	}
}
