// Package raftest holds deterministic test doubles for the raft package.
package raftest

import (
	"sync"
	"time"

	"github.com/nweber23/raftlab/internal/raft"
)

// FakeClock is a raft.Clock whose time moves only when Advance is called.
// Timers fire synchronously, inside Advance, in deadline order (ties: the
// order they were armed).
type FakeClock struct {
	mu     sync.Mutex
	now    time.Duration
	seq    uint64
	timers []*fakeTimer
}

type fakeTimer struct {
	c     *FakeClock
	when  time.Duration
	seq   uint64
	f     func()
	armed bool
}

// NewFakeClock starts at t=0.
func NewFakeClock() *FakeClock { return &FakeClock{} }

// AfterFunc implements raft.Clock.
func (c *FakeClock) AfterFunc(d time.Duration, f func()) raft.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{c: c, f: f}
	c.arm(t, d)
	c.timers = append(c.timers, t)
	return t
}

func (c *FakeClock) arm(t *fakeTimer, d time.Duration) {
	c.seq++
	t.when, t.seq, t.armed = c.now+d, c.seq, true
}

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.armed
	t.armed = false
	return was
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	was := t.armed
	t.c.arm(t, d)
	return was
}

// Advance moves time forward by d, firing every timer that comes due, in
// order. Callbacks run without the clock lock held, so they may re-arm timers;
// a timer re-armed to a deadline inside the window fires in the same call.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	target := c.now + d
	c.mu.Unlock()
	for {
		c.mu.Lock()
		var next *fakeTimer
		for _, t := range c.timers {
			if t.armed && t.when <= target &&
				(next == nil || t.when < next.when || (t.when == next.when && t.seq < next.seq)) {
				next = t
			}
		}
		if next == nil {
			c.now = target
			c.mu.Unlock()
			return
		}
		c.now, next.armed = next.when, false
		c.mu.Unlock()
		next.f()
	}
}
