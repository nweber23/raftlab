package linz

import (
	"fmt"
	"maps"
	"slices"
)

// KV operation kinds, matching the key-value store's.
const (
	KVSet    = "set"
	KVGet    = "get"
	KVDelete = "delete"
)

// KVOp is one completed key-value operation. For a get, Found/Got are what it
// returned; for a delete, Found says whether the key existed; a set returns nothing.
type KVOp struct {
	Client       int
	Call, Return int64
	Kind, Key    string
	Value        string // the value written by a set
	Found        bool
	Got          string
}

type register struct {
	v      string
	exists bool
}

type kvIn struct{ kind, value string }
type kvOut struct {
	found bool
	got   string
}

// registerModel: one key behaves like an atomic register that can be unset.
var registerModel = Model[register, kvIn, kvOut]{
	Step: func(s register, in kvIn, out kvOut) (register, bool) {
		switch in.kind {
		case KVSet:
			return register{in.value, true}, true
		case KVDelete:
			return register{}, out.found == s.exists
		default: // get
			return s, out.found == s.exists && (!s.exists || out.got == s.v)
		}
	},
}

// CheckKV verifies that a key-value history is linearizable. Keys are
// independent registers, so the history is split per key (which is what keeps
// the search tractable) and each part checked on its own. It returns nil when
// every key is linearizable and otherwise an error naming the key and the
// operation that could not be explained.
func CheckKV(history []KVOp) error {
	byKey := map[string][]KVOp{}
	for _, o := range history {
		byKey[o.Key] = append(byKey[o.Key], o)
	}
	for _, key := range slices.Sorted(maps.Keys(byKey)) {
		ops := byKey[key]
		in := make([]Op[kvIn, kvOut], len(ops))
		for i, o := range ops {
			in[i] = Op[kvIn, kvOut]{Client: o.Client, Call: o.Call, Return: o.Return,
				In: kvIn{o.Kind, o.Value}, Out: kvOut{o.Found, o.Got}}
		}
		res, prefix := Check(registerModel, in)
		if res == Linearizable {
			continue
		}
		placed := map[int]bool{}
		for _, i := range prefix {
			placed[i] = true
		}
		culprit, found := -1, false
		for i, o := range ops { // the earliest-returning op the model could not place
			if !placed[i] && (!found || o.Return < ops[culprit].Return) {
				culprit, found = i, true
			}
		}
		return fmt.Errorf("key %q is %v: %d of %d operations linearize; cannot place %s", key, res, len(prefix), len(ops), describe(ops[culprit]))
	}
	return nil
}

func describe(o KVOp) string {
	switch o.Kind {
	case KVSet:
		return fmt.Sprintf("client %d set %q=%q [%d,%d]", o.Client, o.Key, o.Value, o.Call, o.Return)
	case KVDelete:
		return fmt.Sprintf("client %d delete %q (found=%v) [%d,%d]", o.Client, o.Key, o.Found, o.Call, o.Return)
	}
	return fmt.Sprintf("client %d get %q -> %q (found=%v) [%d,%d]", o.Client, o.Key, o.Got, o.Found, o.Call, o.Return)
}
