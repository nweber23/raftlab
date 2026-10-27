// Package transport holds Transport implementations.
package transport

import (
	"sync"

	"github.com/nweber23/raftlab/internal/raft"
)

// Memory is an in-process transport for tests. Send only enqueues; nothing is
// delivered until the test calls DeliverAll, which makes message ordering a
// deterministic function of the test's own steps.
type Memory struct {
	mu       sync.Mutex
	queue    []raft.Message
	handlers map[raft.NodeID]func(raft.Message)
	isolated map[raft.NodeID]bool
}

// NewMemory returns an empty network.
func NewMemory() *Memory {
	return &Memory{
		handlers: make(map[raft.NodeID]func(raft.Message)),
		isolated: make(map[raft.NodeID]bool),
	}
}

// Register installs the receive handler for id.
func (m *Memory) Register(id raft.NodeID, h func(raft.Message)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[id] = h
}

// Send implements raft.Transport.
func (m *Memory) Send(msg raft.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queue = append(m.queue, msg)
}

// Isolate cuts id off: every message to or from it is dropped at delivery
// time, including messages already in flight.
func (m *Memory) Isolate(id raft.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isolated[id] = true
}

// Rejoin undoes Isolate.
func (m *Memory) Rejoin(id raft.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.isolated, id)
}

// DeliverAll drains the queue in FIFO order, including messages enqueued by
// handlers while draining, and returns how many were delivered. It panics
// after 100000 deliveries: that is a message storm, i.e. a bug.
func (m *Memory) DeliverAll() int {
	delivered := 0
	for {
		m.mu.Lock()
		if len(m.queue) == 0 {
			m.mu.Unlock()
			return delivered
		}
		msg := m.queue[0]
		m.queue = m.queue[1:]
		h := m.handlers[msg.To]
		drop := h == nil || m.isolated[msg.From] || m.isolated[msg.To]
		m.mu.Unlock()

		if drop {
			continue
		}
		if delivered++; delivered > 100_000 {
			panic("transport: message storm")
		}
		h(msg) // outside the lock: the handler may Send
	}
}
