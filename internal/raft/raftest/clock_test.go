package raftest

import (
	"testing"
	"time"
)

func TestFakeClockOrderStopReset(t *testing.T) {
	c := NewFakeClock()
	var got []string
	c.AfterFunc(20*time.Millisecond, func() { got = append(got, "b") })
	c.AfterFunc(10*time.Millisecond, func() { got = append(got, "a") })
	stopped := c.AfterFunc(15*time.Millisecond, func() { got = append(got, "x") })
	re := c.AfterFunc(5*time.Millisecond, func() { got = append(got, "r") }).(*fakeTimer)
	stopped.Stop()
	re.Reset(30 * time.Millisecond)
	c.Advance(25 * time.Millisecond)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v", got)
	}
	c.Advance(5 * time.Millisecond)
	if len(got) != 3 || got[2] != "r" {
		t.Fatalf("reset timer: %v", got)
	}
	re.Reset(time.Millisecond) // a fired timer can be re-armed
	c.Advance(time.Millisecond)
	if len(got) != 4 {
		t.Fatalf("re-arm after fire: %v", got)
	}
}
