package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/roman-mazur/design-practice-2-template/httptools"
	"github.com/roman-mazur/design-practice-2-template/signal"
)

var (
	port = flag.Int("port", 8090, "load balancer port")
	// For easier testing it is set to 10 locally via a flag
	timeoutSec     = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	healthInterval = flag.Float64("health-interval", 10, "time between health checks in seconds")
	https          = flag.Bool("https", false, "whether backends support HTTPs")
	traceEnabled   = flag.Bool("trace", false, "whether to include tracing information into responses")
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

type IServer interface {
	Connections() *atomic.Int64
	Health() *atomic.Bool
	Forward(rw http.ResponseWriter, r *http.Request) error
	CheckHealth()
}

// Server structure represents a server and its state
type Server struct {
	Url string
	// We count all connections, except for health checks, made by balancer to the server
	// Counter manipulation is implemented with locking to prevent data races
	connections atomic.Int64
	health      atomic.Bool
}

// Forward() processes a request with a server of choice
func (s *Server) Forward(rw http.ResponseWriter, r *http.Request) error {
	s.Connections().Add(1)
	ctx, cancel := context.WithTimeout(r.Context(), getTimeout())
	defer cancel()
	fwdRequest := r.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = s.Url
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = s.Url

	resp, err := http.DefaultClient.Do(fwdRequest)
	// As soon as we get a response, inform counter about the connection closing
	s.Connections().Add(-1)
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
		s.Health().Store(false)
		rw.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

// Check health updates Server Health parameter
func (s *Server) CheckHealth() {
	ctx, cancel := context.WithTimeout(context.Background(), getTimeout())
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), s.Url), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		s.Health().Store(false)
	} else {
		s.Health().Store(true)
	}
	cancel()
}

func (s *Server) Connections() *atomic.Int64 {
	return &s.connections
}

func (s *Server) Health() *atomic.Bool {
	return &s.health
}

// Balancer contains Server list and decides which one to forward a request to
type Balancer struct {
	ServersPool []IServer
	// The time in seconds between health checks
	CheckInterval float64
}

// balance() returns a pointer to a healthy Server with the least connections
func (b *Balancer) Balance() *IServer {
	pool := b.ServersPool

	var min *IServer = nil
	for i := 0; i < len(pool); i++ {
		if !pool[i].Health().Load() {
			continue
		}
		if min == nil || pool[i].Connections().Load() < (*min).Connections().Load() {
			min = &pool[i]
		}
	}

	return min
}

// StartHealthyService() begins to check and update Server health every 10 seconds
func (b *Balancer) StartHealthService() {
	for i, _ := range b.ServersPool {
		b.ServersPool[i].CheckHealth()
		// Run checks concurrently
		go func(s *IServer) {
			for range time.Tick(time.Duration(b.CheckInterval) * time.Second) {
				(*s).CheckHealth()
			}
		}(&b.ServersPool[i])
	}
}

// Handle() processes an HTTP request to balancer
func (b *Balancer) Handle(rw http.ResponseWriter, r *http.Request) {
	min := b.Balance()
	if min != nil {
		(*min).Forward(rw, r)
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
	serversPool := []IServer{}
	for _, url := range urlList {
		serversPool = append(serversPool, &Server{Url: url})
	}

	balancer := Balancer{serversPool, *healthInterval}

	//Wait for first health check before starting balancer
	balancer.StartHealthService()
	frontend := httptools.CreateServer(*port, http.HandlerFunc(balancer.Handle))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	log.Printf("Timeout: %d seconds", *timeoutSec)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
