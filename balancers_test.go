package http

import (
	"errors"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func Test_NewRoundRobinBalancer(t *testing.T) {
	t.Run("empty hosts", func(t *testing.T) {
		_, err := NewRoundRobinBalancer(nil)
		if !errors.Is(err, ErrNoHostsAvailable) {
			t.Fatalf("expected ErrNoHostsAvailable, got %v", err)
		}
	})

	t.Run("duplicate hosts", func(t *testing.T) {
		_, err := NewRoundRobinBalancer([]string{"http://localhost:4001", "http://localhost:4001"})
		if !errors.Is(err, ErrDuplicateAddresses) {
			t.Fatalf("expected ErrDuplicateAddresses, got %v", err)
		}
	})
}

func Test_RoundRobinBalancer_Next(t *testing.T) {
	rrb, err := NewRoundRobinBalancer([]string{"http://node1:4001", "http://node2:4001", "http://node3:4001"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	exp := []string{
		"http://node1:4001",
		"http://node2:4001",
		"http://node3:4001",
		"http://node1:4001",
		"http://node2:4001",
	}

	for i := range exp {
		u, err := rrb.Next()
		if err != nil {
			t.Fatalf("unexpected error at idx %d: %v", i, err)
		}
		if got := u.String(); got != exp[i] {
			t.Fatalf("expected %s, got %s", exp[i], got)
		}
	}
}

func Test_RoundRobinBalancer_MarkBad(t *testing.T) {
	rrb, err := NewRoundRobinBalancer([]string{"http://node1:4001", "http://node2:4001"})
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	first, err := rrb.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rrb.MarkBad(first)

	for i := 0; i < 5; i++ {
		u, err := rrb.Next()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := u.String(); got == first.String() {
			t.Fatalf("expected bad host %s to be skipped", got)
		}
	}
}

func Test_RoundRobinBalancer_WithHealthRecoversBadHost(t *testing.T) {
	var okHost1 atomic.Bool

	rrb, err := NewRoundRobinBalancerWithHealth(
		[]string{"http://node1:4001", "http://node2:4001"},
		func(u *url.URL) bool {
			if u.String() == "http://node1:4001" {
				return okHost1.Load()
			}
			return true
		},
		10*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	defer rrb.Close()

	u1, err := rrb.Next()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	rrb.MarkBad(u1)

	if got := len(rrb.Bad()); got != 1 {
		t.Fatalf("expected 1 bad host, got %d", got)
	}

	okHost1.Store(true)
	time.Sleep(40 * time.Millisecond)

	if got := len(rrb.Bad()); got != 0 {
		t.Fatalf("expected recovered hosts, got %d bad", got)
	}
}
