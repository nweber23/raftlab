// Package linz checks recorded histories for linearizability, in the style of
// Porcupine and Jepsen's Knossos: a history is linearizable if every operation
// can be assigned a single instant between its call and its return such that
// running the operations in that order against a sequential model reproduces
// every observed output.
//
// The search is the Wing & Gong / Lowe algorithm: depth-first over which
// pending operation to place next, backtracking on a dead end, with a cache of
// (set of placed operations, model state) pairs already shown to fail. The
// problem is NP-complete in general, so Check bounds its work and says so when
// it gives up.
package linz

import (
	"encoding/binary"
	"slices"
)

// Op is one completed operation: what was asked, what came back, and the
// (virtual) times at which the client invoked it and saw the response. Call
// and Return only need to be ordered consistently with real time; an op whose
// outcome the client never learned should be given Return = math.MaxInt64.
type Op[I, O any] struct {
	Client       int
	Call, Return int64
	In           I
	Out          O
}

// Model is a sequential specification. Step applies an operation to state and
// reports whether the observed output is what the model would have produced,
// and the next state. S must be comparable so states can be cached.
type Model[S comparable, I, O any] struct {
	Init S
	Step func(state S, in I, out O) (next S, ok bool)
}

// Result is the verdict of a Check.
type Result int

const (
	// Linearizable means a valid linearization exists.
	Linearizable Result = iota
	// Illegal means none exists: the system violated the model.
	Illegal
	// Unknown means the search hit its budget before deciding.
	Unknown
)

func (r Result) String() string { return [...]string{"linearizable", "illegal", "unknown"}[r] }

// MaxSteps bounds the number of placement attempts per Check.
const MaxSteps = 20_000_000

type entry struct {
	op         int
	isCall     bool
	prev, next *entry
	match      *entry // for a call: its return
}

// Check decides whether ops is linearizable with respect to m. The second
// result is the longest valid linearization prefix the search found (indexes
// into ops, in order): on Illegal, the operation that comes next is the one
// the model cannot explain, which is usually the place to start looking.
func Check[S comparable, I, O any](m Model[S, I, O], ops []Op[I, O]) (Result, []int) {
	if len(ops) == 0 {
		return Linearizable, nil
	}
	type frame struct {
		e     *entry
		state S
	}
	// Build the time-ordered event list. At equal times calls sort first, so
	// operations that touch are treated as concurrent: that can only make the
	// checker more permissive, never raise a false alarm.
	type ev struct {
		t      int64
		isCall bool
		op     int
	}
	events := make([]ev, 0, 2*len(ops))
	for i, o := range ops {
		events = append(events, ev{o.Call, true, i}, ev{o.Return, false, i})
	}
	slices.SortStableFunc(events, func(a, b ev) int {
		switch {
		case a.t != b.t:
			if a.t < b.t {
				return -1
			}
			return 1
		case a.isCall != b.isCall:
			if a.isCall {
				return -1
			}
			return 1
		}
		return a.op - b.op
	})
	head := &entry{}
	last := head
	calls := make([]*entry, len(ops))
	for _, e := range events {
		n := &entry{op: e.op, isCall: e.isCall, prev: last}
		last.next = n
		last = n
		if e.isCall {
			calls[e.op] = n
		} else {
			calls[e.op].match = n
		}
	}

	lift := func(c *entry) { // remove a call and its return from the list
		c.prev.next = c.next
		c.next.prev = c.prev // a call is never last: its return follows
		r := c.match
		r.prev.next = r.next
		if r.next != nil {
			r.next.prev = r.prev
		}
	}
	unlift := func(c *entry) {
		r := c.match
		r.prev.next = r
		if r.next != nil {
			r.next.prev = r
		}
		c.prev.next = c
		c.next.prev = c
	}

	type key struct {
		placed string
		state  S
	}
	words := (len(ops) + 63) / 64
	placed := make([]uint64, words)
	buf := make([]byte, 0, 8*words)
	bitsKey := func(w []uint64) string {
		buf = buf[:0]
		for _, x := range w {
			buf = binary.LittleEndian.AppendUint64(buf, x)
		}
		return string(buf)
	}
	seen := map[key]struct{}{{bitsKey(placed), m.Init}: {}}

	state := m.Init
	var stack []frame
	var best []int // ops placed at the deepest point reached
	depthBest := -1
	steps := 0
	for e := head.next; head.next != nil; {
		if steps++; steps > MaxSteps {
			return Unknown, best
		}
		if e.isCall {
			o := ops[e.op]
			if next, ok := m.Step(state, o.In, o.Out); ok {
				placed[e.op/64] |= 1 << (e.op % 64)
				k := key{bitsKey(placed), next}
				if _, dup := seen[k]; !dup {
					seen[k] = struct{}{}
					stack = append(stack, frame{e, state})
					if len(stack) > depthBest {
						depthBest = len(stack)
						best = best[:0]
						for _, f := range stack {
							best = append(best, f.e.op)
						}
					}
					state = next
					lift(e)
					e = head.next
					continue
				}
				placed[e.op/64] &^= 1 << (e.op % 64)
			}
			e = e.next
			continue
		}
		// A return came up while some operation before it is still unplaced
		// and could not be: this branch is dead. Undo the last placement.
		if len(stack) == 0 {
			return Illegal, best
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = f.state
		placed[f.e.op/64] &^= 1 << (f.e.op % 64)
		unlift(f.e)
		e = f.e.next
	}
	return Linearizable, nil
}
