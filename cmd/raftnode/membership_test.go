package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A real multi-process cluster grows by one server and shrinks by another:
// the newcomer is told the members only through -join, everyone else learns its
// address from the log.
func TestProcessesChangeMembership(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and starts processes")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "raftnode")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	addr := map[int]string{}
	for i := 1; i <= 4; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr[i] = l.Addr().String()
		_ = l.Close()
	}
	peers := func(ids ...int) string {
		var s []string
		for _, i := range ids {
			s = append(s, fmt.Sprintf("%d=%s", i, addr[i]))
		}
		return strings.Join(s, ",")
	}
	start := func(id int, extra ...string) {
		args := append([]string{"-id", fmt.Sprint(id), "-data", filepath.Join(dir, "data")}, extra...)
		cmd := exec.Command(bin, args...)
		cmd.Env = append(os.Environ(), "RAFTLAB_TOKEN=tok", "RAFTLAB_ADMIN_TOKEN=adm")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	}
	for i := 1; i <= 3; i++ {
		start(i, "-peers", peers(1, 2, 3))
	}
	start(4, "-join", "-peers", peers(1, 2, 3, 4))

	// The client follows redirects and keeps the bearer token across them.
	client := &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		req.Header = via[0].Header.Clone()
		return nil
	}}
	do := func(method string, id int, path, body string) (int, string) {
		req, _ := http.NewRequest(method, "http://"+addr[id]+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer adm")
		resp, err := client.Do(req)
		if err != nil {
			return 0, err.Error()
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	eventually := func(what string, cond func() bool) {
		t.Helper()
		for end := time.Now().Add(15 * time.Second); time.Now().Before(end); time.Sleep(100 * time.Millisecond) {
			if cond() {
				return
			}
		}
		t.Fatalf("timed out: %s", what)
	}
	eventually("write accepted", func() bool { c, _ := do("PUT", 1, "/kv/a", "hello"); return c == 204 })

	eventually("join accepted", func() bool {
		c, _ := do("POST", 1, "/membership", fmt.Sprintf(`{"1":%q,"2":%q,"3":%q,"4":%q}`, addr[1], addr[2], addr[3], addr[4]))
		return c == http.StatusAccepted
	})
	eventually("newcomer serves the old data", func() bool { c, b := do("GET", 4, "/kv/a", ""); return c == 200 && b == "hello" })

	eventually("removal accepted", func() bool {
		c, _ := do("POST", 2, "/membership", fmt.Sprintf(`{"2":%q,"3":%q,"4":%q}`, addr[2], addr[3], addr[4]))
		return c == http.StatusAccepted
	})
	eventually("removed server learns it is gone", func() bool {
		_, b := do("GET", 1, "/status", "")
		return strings.Contains(b, `"voters":[2,3,4]`)
	})
	req, _ := http.NewRequest("POST", "http://"+addr[2]+"/membership", strings.NewReader("{}"))
	req.Header.Set("Authorization", "Bearer tok") // the peer token must not work here
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("peer token accepted for membership: %v %v", resp, err)
	}
	if c, _ := do("POST", 2, "/membership", "{}"); c != http.StatusBadRequest {
		t.Fatalf("empty membership: %d", c)
	}
}
