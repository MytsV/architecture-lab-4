package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var (
	port = flag.Int("port", 8090, "load balancer port")
	// For easier testing it is set to 10 locally via a flag
	timeoutSec   = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https        = flag.Bool("https", false, "whether backends support HTTPs")
	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
	timeout      = time.Duration(*timeoutSec) * time.Second
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

// Server structure represents a server and its state
type Server struct {
	Url string
	// We count all connections but health checks made by balancer to the server
	// Counter manipulation is implemented with locking to prevent data races
	ConnectionCnt Counter
	// As changing IsHealthy is an atomic operation, we don't need to synchronize it
	IsHealthy bool
}

// Counter prevents data races and can be used from several goroutines
type Counter struct {
	mu sync.Mutex
	v  int
}

func (c *Counter) Change(amount int) {
	c.mu.Lock()
	c.v += amount
	c.mu.Unlock()
}

func (c *Counter) Get() int {
	c.mu.Lock()
	res := c.v
	c.mu.Unlock()
	return res
}

// Forward processes a request with a server of choice
func (s *Server) Forward(rw http.ResponseWriter, r *http.Request) error {
	s.ConnectionCnt.Change(1)
	ctx, _ := context.WithTimeout(r.Context(), timeout)
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = s.Url
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = s.Url

	resp, err := http.DefaultClient.Do(fwdRequest)
	// As soon as we get a response, inform counter about the connection closing
	s.ConnectionCnt.Change(-1)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				rw.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			rw.Header().Set("lb-from", s.Url)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		rw.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(rw, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", s.Url, err)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

// Check health processes server state and changes IsHealthy parameter
func (s *Server) CheckHealth() {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), s.Url), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		s.IsHealthy = false
	}
	s.IsHealthy = true
}

// Balancer contains Server list and decides which one to forward a request to
type Balancer struct {
	serversPool []Server
}

// balance returns a pointer to a healthy Server with the least connections
func (b *Balancer) balance() *Server {
	pool := b.serversPool

	var min *Server = nil
	for i := 0; i < len(pool); i++ {
		if !pool[i].IsHealthy {
			continue
		}
		if min == nil || pool[i].ConnectionCnt.Get() < min.ConnectionCnt.Get() {
			min = &pool[i]
		}
	}

	return min
}

// StartHealthyService begins to check and update Server health every 10 seconds
func (b *Balancer) StartHealthService() {
	for i, _ := range b.serversPool {
		// Run checks concurrently
		go func(s *Server) {
			for range time.Tick(10 * time.Second) {
				s.CheckHealth()
			}
		}(&b.serversPool[i])
	}
}

// Handle processes an HTTP request to balancer
func (b *Balancer) Handle(rw http.ResponseWriter, r *http.Request) {
	min := b.balance()
	if min != nil {
		min.Forward(rw, r)
	} else {

	}
}

func main() {
	flag.Parse()

	balancer := Balancer{
		[]Server{
			{Url: "server1:8080", IsHealthy: true},
			{Url: "server2:8080", IsHealthy: false},
			{Url: "server3:8080", IsHealthy: false},
		},
	}

	balancer.StartHealthService()
	frontend := httptools.CreateServer(*port, http.HandlerFunc(balancer.Handle))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
