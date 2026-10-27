// Package clock provides the wall-clock implementation of raft.Clock.
package clock

import (
	"time"

	"github.com/nweber23/raftlab/internal/raft"
)

// System is a raft.Clock backed by the time package.
type System struct{}

// AfterFunc implements raft.Clock.
func (System) AfterFunc(d time.Duration, f func()) raft.Timer { return time.AfterFunc(d, f) }
