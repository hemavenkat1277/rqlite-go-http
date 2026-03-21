package http

import (
	"errors"
	"math/rand/v2"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrNoHostsAvailable is returned when no hosts are available.
	ErrNoHostsAvailable = errors.New("no hosts available")

	// ErrDuplicateAddresses is returned when duplicate addresses are provided
	// to a balancer.
	ErrDuplicateAddresses = errors.New("duplicate addresses provided")
)

// LoopbackBalancer takes a single address and always returns it when Next() is called.
// It performs no healthchecking.
type LoopbackBalancer struct {
	u *url.URL
}

// RoundRobinBalancer takes a list of addresses and returns them one-by-one in
// round-robin order when Next() is called.
type RoundRobinBalancer struct {
	mu      sync.RWMutex
	hosts   map[string]*Host
	ordered []string
	next    atomic.Uint64

	chkInterval time.Duration
	chckFn      HostChecker
	ch          chan *url.URL

	wg   sync.WaitGroup
	done chan struct{}

	closeOnce sync.Once
}

// NewLoopbackBalancer returns a new LoopbackBalancer.
func NewLoopbackBalancer(address string) (*LoopbackBalancer, error) {
	u, err := url.Parse(address)
	if err != nil {
		return nil, err
	}

	return &LoopbackBalancer{
		u: u,
	}, nil
}

// Next returns the next address in the list of addresses.
func (lb *LoopbackBalancer) Next() (*url.URL, error) {
	return lb.u, nil
}

// NewRoundRobinBalancer returns a new RoundRobinBalancer.
func NewRoundRobinBalancer(addresses []string) (*RoundRobinBalancer, error) {
	return newRoundRobinBalancer(addresses, nil, 0)
}

// NewRoundRobinBalancerWithHealth returns a round-robin balancer with health
// checking support. Hosts marked bad via MarkBad() are periodically rechecked
// and moved back into rotation once healthy.
func NewRoundRobinBalancerWithHealth(addresses []string, chckFn HostChecker, d time.Duration) (*RoundRobinBalancer, error) {
	return newRoundRobinBalancer(addresses, chckFn, d)
}

func newRoundRobinBalancer(addresses []string, chckFn HostChecker, d time.Duration) (*RoundRobinBalancer, error) {
	seen := make(map[string]struct{}, len(addresses))
	hosts := make(map[string]*Host, len(addresses))
	ordered := make([]string, 0, len(addresses))

	for _, s := range addresses {
		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[u.String()]; ok {
			return nil, ErrDuplicateAddresses
		}
		seen[u.String()] = struct{}{}
		hosts[u.String()] = &Host{URL: u, Healthy: true}
		ordered = append(ordered, u.String())
	}

	if len(ordered) == 0 {
		return nil, ErrNoHostsAvailable
	}

	rrb := &RoundRobinBalancer{
		hosts:       hosts,
		ordered:     ordered,
		chkInterval: d,
		chckFn:      chckFn,
		done:        make(chan struct{}),
	}

	if chckFn != nil && d > 0 {
		rrb.ch = make(chan *url.URL, len(ordered))
		rrb.wg.Add(2)
		go rrb.checkBadHosts()
		go rrb.markGoodHosts()
	}

	return rrb, nil
}

// Next returns the next address in round-robin order.
func (rrb *RoundRobinBalancer) Next() (*url.URL, error) {
	rrb.mu.RLock()
	defer rrb.mu.RUnlock()

	if len(rrb.ordered) == 0 {
		return nil, ErrNoHostsAvailable
	}

	start := rrb.next.Add(1) - 1
	for i := 0; i < len(rrb.ordered); i++ {
		idx := (int(start) + i) % len(rrb.ordered)
		h := rrb.hosts[rrb.ordered[idx]]
		if h.Healthy {
			return h.URL, nil
		}
	}

	return nil, ErrNoHostsAvailable
}

// MarkBad marks an address returned by Next() as bad. The balancer will skip
// it until it is considered healthy again.
func (rrb *RoundRobinBalancer) MarkBad(u *url.URL) {
	rrb.mu.Lock()
	defer rrb.mu.Unlock()
	h, ok := rrb.hosts[u.String()]
	if !ok {
		return
	}
	h.Healthy = false
}

// Healthy returns the slice of currently healthy hosts.
func (rrb *RoundRobinBalancer) Healthy() []*url.URL {
	rrb.mu.RLock()
	defer rrb.mu.RUnlock()
	var healthy []*url.URL
	for _, k := range rrb.ordered {
		if rrb.hosts[k].Healthy {
			healthy = append(healthy, rrb.hosts[k].URL)
		}
	}
	return healthy
}

// Bad returns the slice of currently bad hosts.
func (rrb *RoundRobinBalancer) Bad() []*url.URL {
	rrb.mu.RLock()
	defer rrb.mu.RUnlock()
	var bad []*url.URL
	for _, k := range rrb.ordered {
		if !rrb.hosts[k].Healthy {
			bad = append(bad, rrb.hosts[k].URL)
		}
	}
	return bad
}

// Close closes the RoundRobinBalancer. A closed balancer should not be reused.
func (rrb *RoundRobinBalancer) Close() {
	if rrb.chckFn == nil || rrb.chkInterval <= 0 {
		return
	}
	rrb.closeOnce.Do(func() {
		close(rrb.done)
		rrb.wg.Wait()
	})
}

func (rrb *RoundRobinBalancer) checkBadHosts() {
	defer rrb.wg.Done()
	ticker := time.NewTicker(rrb.chkInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rrb.mu.RLock()
			for _, k := range rrb.ordered {
				host := rrb.hosts[k]
				if !host.Healthy {
					if ok := rrb.chckFn(host.URL); ok {
						rrb.ch <- host.URL
					}
				}
			}
			rrb.mu.RUnlock()
		case <-rrb.done:
			return
		}
	}
}

func (rrb *RoundRobinBalancer) markGoodHosts() {
	defer rrb.wg.Done()
	for {
		select {
		case u := <-rrb.ch:
			rrb.mu.Lock()
			if h, ok := rrb.hosts[u.String()]; ok {
				h.Healthy = true
			}
			rrb.mu.Unlock()
		case <-rrb.done:
			return
		}
	}
}

// Host represents a URL and its health status.
type Host struct {
	URL     *url.URL
	Healthy bool
}

// HostChecker is a function that takes a URL and returns true if the URL is
// healthy.
type HostChecker func(url *url.URL) bool

// RandomBalancer takes a list of addresses and returns a random one from its
// healthy list when Next() is called. At the start all supplied addresses are
// considered healthy. If a client detects that an address is unhealthy, it can
// call MarkBad() to mark the address as unhealthy. The RandomBalancer will
// then periodically check the health of the address and mark it as healthy
// again if and when it becomes healthy.
type RandomBalancer struct {
	mu    sync.RWMutex
	hosts map[string]*Host

	chkInterval time.Duration
	chckFn      HostChecker
	ch          chan *url.URL

	wg   sync.WaitGroup
	done chan struct{}
}

// NewRandomBalancer returns a new RandomBalancer.
func NewRandomBalancer(urls []string, chckFn HostChecker, d time.Duration) (*RandomBalancer, error) {
	hosts := make(map[string]*Host)
	for _, s := range urls {
		u, err := url.Parse(s)
		if err != nil {
			return nil, err
		}
		if _, ok := hosts[u.String()]; ok {
			return nil, ErrDuplicateAddresses
		}
		hosts[u.String()] = &Host{URL: u, Healthy: true}
	}
	if len(hosts) == 0 {
		return nil, ErrNoHostsAvailable
	}
	rb := &RandomBalancer{
		hosts:       hosts,
		chkInterval: d,
		chckFn:      chckFn,
		ch:          make(chan *url.URL, len(hosts)),
		done:        make(chan struct{}),
	}

	rb.wg.Add(2)
	go rb.checkBadHosts()
	go rb.markGoodHosts()
	return rb, nil
}

// Next returns a random address from the list of addresses it currently
// considers healthy.
func (rb *RandomBalancer) Next() (*url.URL, error) {
	healthy := rb.Healthy()
	if len(healthy) == 0 {
		return nil, ErrNoHostsAvailable
	}
	idx := rand.IntN(len(healthy))
	return healthy[idx], nil
}

// MarkBad marks an address returned by Next() as bad. The RandomBalancer
// will not return this address until the RandomBalancer considers it healthy
// again.
func (rb *RandomBalancer) MarkBad(u *url.URL) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.hosts[u.String()].Healthy = false
}

// Healthy returns the slice of currently healthy hosts.
func (rb *RandomBalancer) Healthy() []*url.URL {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	var healthy []*url.URL
	for _, host := range rb.hosts {
		if host.Healthy {
			healthy = append(healthy, host.URL)
		}
	}
	return healthy
}

// Bad returns the slice of currently bad hosts.
func (rb *RandomBalancer) Bad() []*url.URL {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	var bad []*url.URL
	for _, host := range rb.hosts {
		if !host.Healthy {
			bad = append(bad, host.URL)
		}
	}
	return bad
}

// Close closes the RandomBalancer. A closed RandomBalancer should not be reused.
func (rb *RandomBalancer) Close() {
	close(rb.done)
	rb.wg.Wait()
}

func (rb *RandomBalancer) checkBadHosts() {
	defer rb.wg.Done()
	ticker := time.NewTicker(rb.chkInterval)
	for {
		select {
		case <-ticker.C:
			rb.mu.RLock()
			for _, host := range rb.hosts {
				if !host.Healthy {
					if ok := rb.chckFn(host.URL); ok {
						rb.ch <- host.URL
					}
				}
			}
			rb.mu.RUnlock()
		case <-rb.done:
			return
		}
	}
}

func (rb *RandomBalancer) markGoodHosts() {
	defer rb.wg.Done()
	for {
		select {
		case u := <-rb.ch:
			rb.mu.Lock()
			for _, host := range rb.hosts {
				if host.URL == u {
					host.Healthy = true
					break
				}
			}
			rb.mu.Unlock()
		case <-rb.done:
			return
		}
	}
}
