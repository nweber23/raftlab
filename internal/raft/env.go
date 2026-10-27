package raft

import "time"

// Transport sends messages to other nodes. Send must not block and must not
// call back into the sending Node synchronously: delivery is the transport's
// business, and it may drop, delay or reorder messages.
type Transport interface {
	Send(m Message)
}

// Timer is the subset of *time.Timer the core needs.
type Timer interface {
	Stop() bool
	Reset(d time.Duration) bool
}

// Clock is the core's only source of time. Production code backs it with
// time.AfterFunc; tests back it with a fake that advances only on demand.
type Clock interface {
	AfterFunc(d time.Duration, f func()) Timer
}

// StateMachine is the replicated application. Apply runs inside the node's
// critical section, in log order, exactly once per committed non-empty
// command; it must be fast and must not call back into the Node.
type StateMachine interface {
	Apply(index uint64, command []byte)
}

type noopMachine struct{}

func (noopMachine) Apply(uint64, []byte) {}
