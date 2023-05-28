package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
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
	// Pass server pool as a parameter for easy local run (in order to check for data races, for example)
	serverUrls = flag.String(
		"servers",
		"server1:8080,server2:8080,server3:8080",
		"comma separated list of server URLs",
	)
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func getTimeout() time.Duration {
	return time.Duration(*timeoutSec) * time.Second
}

// Server structure represents a server and its state
type Server struct {
	Url string
	// We count all connections but health checks made by balancer to the server
	// Counter manipulation is implemented with locking to prevent data races
	ConnectionCnt Counter
	IsHealthy     SyncBool
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

type SyncBool struct {
	mu sync.Mutex
	v  bool
}

func (c *SyncBool) Set(value bool) {
	c.mu.Lock()
	c.v = value
	c.mu.Unlock()
}

func (c *SyncBool) Get() bool {
	c.mu.Lock()
	res := c.v
	c.mu.Unlock()
	return res
}

// Forward processes a request with a server of choice
func (s *Server) Forward(rw http.ResponseWriter, r *http.Request) error {
	s.ConnectionCnt.Change(1)
	ctx, _ := context.WithTimeout(r.Context(), getTimeout())
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
		s.IsHealthy.Set(false)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

// Check health processes server state and changes IsHealthy parameter
func (s *Server) CheckHealth() {
	ctx, _ := context.WithTimeout(context.Background(), getTimeout())
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), s.Url), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		s.IsHealthy.Set(false)
	} else {
		s.IsHealthy.Set(true)
	}
}

// Balancer contains Server list and decides which one to forward a request to
type Balancer struct {
	ServersPool []Server
}

// balance returns a pointer to a healthy Server with the least connections
func (b *Balancer) balance() *Server {
	pool := b.ServersPool

	var min *Server = nil
	for i := 0; i < len(pool); i++ {
		if !pool[i].IsHealthy.Get() {
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
	for i, _ := range b.ServersPool {
		b.ServersPool[i].CheckHealth()
		// Run checks concurrently
		go func(s *Server) {
			for range time.Tick(10 * time.Second) {
				s.CheckHealth()
			}
		}(&b.ServersPool[i])
	}
}

// Handle processes an HTTP request to balancer
func (b *Balancer) Handle(rw http.ResponseWriter, r *http.Request) {
	min := b.balance()
	if min != nil {
		min.Forward(rw, r)
	} else {
		error := "Request handling error: all servers are out of reach"
		log.Println(error)
		rw.WriteHeader(http.StatusServiceUnavailable)
		rw.Write([]byte(error))
	}
}

func main() {
	flag.Parse()
	urlList := strings.Split(*serverUrls, ",")
	serversPool := []Server{}
	for _, url := range urlList {
		serversPool = append(serversPool, Server{Url: url})
	}

	balancer := Balancer{serversPool}

	//Wait for first health check before starting balancer
	balancer.StartHealthService()
	frontend := httptools.CreateServer(*port, http.HandlerFunc(balancer.Handle))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	log.Printf("Timeout: %d seconds", *timeoutSec)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
