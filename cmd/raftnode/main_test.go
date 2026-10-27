package main

import "testing"

func TestParsePeers(t *testing.T) {
	p, err := parsePeers("1=a:1, 2=b:2")
	if err != nil || len(p) != 2 || p[2] != "b:2" {
		t.Fatalf("%v %v", p, err)
	}
	for _, bad := range []string{"", "1", "x=a:1", "0=a:1", "1="} {
		if _, err := parsePeers(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
