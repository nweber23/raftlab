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
