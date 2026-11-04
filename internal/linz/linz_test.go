package linz

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
)

func set(c int, call, ret int64, k, v string) KVOp {
	return KVOp{Client: c, Call: call, Return: ret, Kind: KVSet, Key: k, Value: v}
}
func get(c int, call, ret int64, k, got string, found bool) KVOp {
	return KVOp{Client: c, Call: call, Return: ret, Kind: KVGet, Key: k, Got: got, Found: found}
}
func del(c int, call, ret int64, k string, found bool) KVOp {
	return KVOp{Client: c, Call: call, Return: ret, Kind: KVDelete, Key: k, Found: found}
}

func TestKnownHistories(t *testing.T) {
	for name, tc := range map[string]struct {
		h  []KVOp
		ok bool
	}{
		"empty":                       {nil, true},
		"read of unset key":           {[]KVOp{get(1, 0, 1, "k", "", false)}, true},
		"sequential":                  {[]KVOp{set(1, 0, 1, "k", "a"), get(1, 2, 3, "k", "a", true), set(1, 4, 5, "k", "b"), get(2, 6, 7, "k", "b", true)}, true},
		"stale read after ack":        {[]KVOp{set(1, 0, 1, "k", "a"), set(1, 2, 3, "k", "b"), get(2, 4, 5, "k", "a", true)}, false},
		"read of never-written":       {[]KVOp{set(1, 0, 1, "k", "a"), get(2, 2, 3, "k", "zzz", true)}, false},
		"concurrent read old":         {[]KVOp{set(1, 0, 10, "k", "a"), get(2, 2, 3, "k", "", false)}, true},
		"concurrent read new":         {[]KVOp{set(1, 0, 10, "k", "a"), get(2, 2, 3, "k", "a", true)}, true},
		"new then old (impossible)":   {[]KVOp{set(1, 0, 10, "k", "a"), get(2, 1, 2, "k", "a", true), get(3, 3, 4, "k", "", false)}, false},
		"delete then read gone":       {[]KVOp{set(1, 0, 1, "k", "a"), del(1, 2, 3, "k", true), get(2, 4, 5, "k", "", false)}, true},
		"read resurrects deleted":     {[]KVOp{set(1, 0, 1, "k", "a"), del(1, 2, 3, "k", true), get(2, 4, 5, "k", "a", true)}, false},
		"delete lies":                 {[]KVOp{del(1, 0, 1, "k", true)}, false},
		"two writers any order":       {[]KVOp{set(1, 0, 5, "k", "a"), set(2, 0, 5, "k", "b"), get(3, 6, 7, "k", "a", true)}, true},
		"keys are independent":        {[]KVOp{set(1, 0, 1, "x", "1"), set(1, 2, 3, "y", "2"), get(2, 4, 5, "x", "1", true), get(2, 6, 7, "y", "2", true)}, true},
		"touching ops are concurrent": {[]KVOp{set(1, 0, 5, "k", "a"), get(2, 5, 6, "k", "", false)}, true},
	} {
		err := CheckKV(tc.h)
		if (err == nil) != tc.ok {
			t.Errorf("%s: got err=%v, want ok=%v", name, err, tc.ok)
		}
	}
}

func TestReportsTheOffendingOperation(t *testing.T) {
	err := CheckKV([]KVOp{set(1, 0, 1, "k", "a"), set(1, 2, 3, "k", "b"), get(2, 4, 5, "k", "a", true)})
	if err == nil {
		t.Fatal("stale read accepted")
	}
	t.Log(err)
	if want := `get "k" -> "a"`; !contains(err.Error(), want) {
		t.Fatalf("error should name the stale read (%s): %v", want, err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// A history produced by really running operations against an atomic register
// is linearizable by construction, however much the intervals overlap.
func randomLinearizable(rng *rand.Rand, n int) []KVOp {
	var h []KVOp
	cur := register{}
	for i := 0; i < n; i++ {
		t := int64(i * 10)
		o := KVOp{Client: rng.IntN(5), Key: "k", Call: t - int64(rng.IntN(60)), Return: t + int64(rng.IntN(60))}
		switch rng.IntN(4) {
		case 0, 1:
			o.Kind, o.Value = KVSet, fmt.Sprint("v", i)
			cur = register{o.Value, true}
		case 2:
			o.Kind, o.Found, o.Got = KVGet, cur.exists, cur.v
		default:
			o.Kind, o.Found = KVDelete, cur.exists
			cur = register{}
		}
		h = append(h, o)
	}
	return h
}

func TestRandomLinearizableHistoriesPass(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 200; i++ {
		if err := CheckKV(randomLinearizable(rng, 12+rng.IntN(40))); err != nil {
			t.Fatalf("history %d: %v", i, err)
		}
	}
}

// Corrupt one read to return a value nobody wrote: no linearization can exist.
func TestCorruptedHistoriesFail(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	checked := 0
	for i := 0; i < 200; i++ {
		h := randomLinearizable(rng, 30)
		for j := range h {
			if h[j].Kind == KVGet {
				h[j].Got, h[j].Found = "never-written", true
				if err := CheckKV(h); err == nil {
					t.Fatalf("history %d: corrupted read at %d accepted", i, j)
				}
				checked++
				break
			}
		}
	}
	if checked < 100 {
		t.Fatalf("only %d histories had a read to corrupt", checked)
	}
}

// Fourteen fully overlapping writers: 2^14 subsets x 14 values to rule out.
// The cache is what keeps this from being 14! orderings. (The search is still
// exponential in concurrency; histories here never exceed a handful of clients.)
func TestHeavyConcurrencyStaysTractable(t *testing.T) {
	var h []KVOp
	for i := 0; i < 14; i++ {
		h = append(h, set(i, 0, 100, "k", fmt.Sprint("v", i)))
	}
	h = append(h, get(99, 101, 102, "k", "v7", true))
	if err := CheckKV(h); err != nil {
		t.Fatal(err)
	}
	h[len(h)-1] = get(99, 101, 102, "k", "nobody", true)
	if err := CheckKV(h); err == nil {
		t.Fatal("read of an unwritten value accepted")
	}
}

func TestOperationWithUnknownOutcome(t *testing.T) {
	// A write whose ack never arrived may or may not have happened.
	pending := set(1, 0, math.MaxInt64, "k", "maybe")
	if err := CheckKV([]KVOp{pending, get(2, 5, 6, "k", "maybe", true)}); err != nil {
		t.Fatalf("a pending write must be allowed to take effect: %v", err)
	}
	if err := CheckKV([]KVOp{pending, get(2, 5, 6, "k", "", false)}); err != nil {
		t.Fatalf("a pending write must be allowed to never take effect: %v", err)
	}
}
