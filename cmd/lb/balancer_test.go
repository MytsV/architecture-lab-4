package main

import (
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Test Balance function for small pool of servers
func TestBalancer_Balance_Table(t *testing.T) {
	type testCase struct {
		name      string
		pool      []IServer
		resultIdx int
	}
	testCases := []testCase{
		{
			name: "Chooses first of servers with equal connection count (1)",
			pool: []IServer{
				&mockServer{Url: "0", connections: Counter{v: 0}, health: SyncBool{v: true}},
				&mockServer{Url: "1", connections: Counter{v: 0}, health: SyncBool{v: true}},
				&mockServer{Url: "2", connections: Counter{v: 0}, health: SyncBool{v: true}},
			},
			resultIdx: 0,
		},
		{
			name: "Chooses first of servers with equal connection count (2)",
			pool: []IServer{
				&mockServer{Url: "0", connections: Counter{v: 1}, health: SyncBool{v: true}},
				&mockServer{Url: "1", connections: Counter{v: 1}, health: SyncBool{v: true}},
				&mockServer{Url: "2", connections: Counter{v: 0}, health: SyncBool{v: true}},
				&mockServer{Url: "3", connections: Counter{v: 0}, health: SyncBool{v: true}},
			},
			resultIdx: 2,
		},
		{
			name: "Finds the correct server in the beginning",
			pool: []IServer{
				&mockServer{Url: "0", connections: Counter{v: 0}, health: SyncBool{v: true}},
				&mockServer{Url: "1", connections: Counter{v: 3}, health: SyncBool{v: true}},
				&mockServer{Url: "2", connections: Counter{v: 4}, health: SyncBool{v: true}},
			},
			resultIdx: 0,
		},
		{
			name: "Finds the correct server in the middle (1)",
			pool: []IServer{
				&mockServer{Url: "0", connections: Counter{v: 1}, health: SyncBool{v: true}},
				&mockServer{Url: "1", connections: Counter{v: 4}, health: SyncBool{v: true}},
				&mockServer{Url: "2", connections: Counter{v: 6}, health: SyncBool{v: true}},
				&mockServer{Url: "3", connections: Counter{v: 0}, health: SyncBool{v: true}},
				&mockServer{Url: "4", connections: Counter{v: 4}, health: SyncBool{v: true}},
			},
			resultIdx: 3,
		},
		{
			name: "Finds the correct server in the middle (2)",
			pool: []IServer{
				&mockServer{Url: "0", connections: Counter{v: 3}, health: SyncBool{v: true}},
				&mockServer{Url: "1", connections: Counter{v: 2}, health: SyncBool{v: true}},
				&mockServer{Url: "2", connections: Counter{v: 4}, health: SyncBool{v: true}},
			},
			resultIdx: 1,
		},
		{
			name: "Finds the correct server in the end",
			pool: []IServer{
				&mockServer{Url: "0", connections: Counter{v: 3}, health: SyncBool{v: true}},
				&mockServer{Url: "1", connections: Counter{v: 2}, health: SyncBool{v: true}},
				&mockServer{Url: "2", connections: Counter{v: 1}, health: SyncBool{v: true}},
			},
			resultIdx: 2,
		},
		{
			name:      "Returns nil if there are no servers",
			pool:      []IServer{},
			resultIdx: -1,
		},
		{
			name: "Ignores unhealthy servers (1)",
			pool: []IServer{
				&mockServer{Url: "0", connections: Counter{v: 0}, health: SyncBool{v: false}},
				&mockServer{Url: "1", connections: Counter{v: 0}, health: SyncBool{v: true}},
				&mockServer{Url: "2", connections: Counter{v: 0}, health: SyncBool{v: true}},
			},
			resultIdx: 1,
		},
		{
			name: "Ignores unhealthy servers (2)",
			pool: []IServer{
				&mockServer{Url: "0", connections: Counter{v: 0}, health: SyncBool{v: false}},
				&mockServer{Url: "1", connections: Counter{v: 0}, health: SyncBool{v: false}},
				&mockServer{Url: "2", connections: Counter{v: 0}, health: SyncBool{v: false}},
			},
			resultIdx: -1,
		},
		{
			name: "Ignores unhealthy servers (3)",
			pool: []IServer{
				&mockServer{Url: "0", connections: Counter{v: 1}, health: SyncBool{v: false}},
				&mockServer{Url: "1", connections: Counter{v: 0}, health: SyncBool{v: false}},
				&mockServer{Url: "2", connections: Counter{v: 10}, health: SyncBool{v: true}},
				&mockServer{Url: "3", connections: Counter{v: 9}, health: SyncBool{v: true}},
				&mockServer{Url: "4", connections: Counter{v: 2}, health: SyncBool{v: false}},
			},
			resultIdx: 3,
		},
	}

	for _, c := range testCases {
		balancer := Balancer{ServersPool: c.pool}
		res := balancer.Balance()
		if c.resultIdx >= 0 {
			assert.NotNil(t, res, c.name)
			if res != nil {
				assert.Equal(t, balancer.ServersPool[c.resultIdx], *res, c.name)
			}
		} else {
			assert.Nil(t, res, c.name)
		}
	}
}

/*
Check if putting correct server in the middle of the list works.
Test if big server pools are processed correctly
*/
func TestBalancer_Balance_Random(t *testing.T) {
	const testCount = 100
	const maxLength = 1000

	for i := 0; i < testCount; i++ {
		length := rand.Intn(maxLength) + 1
		minConnection := rand.Intn(length)
		pool := []IServer{}
		for j := 0; j < length; j++ {
			conn := 1
			if minConnection == j {
				conn = 0
			}
			pool = append(pool, &mockServer{
				Url:         strconv.Itoa(j),
				connections: Counter{v: conn},
				health:      SyncBool{v: true},
			})
		}
		balancer := Balancer{ServersPool: pool}
		res := balancer.Balance()
		assert.NotNil(t, res)
		if res != nil {
			assert.Equal(t, balancer.ServersPool[minConnection], *res)
		}
	}
}

/*
Test if connection count is used correctly;
If multiple requests come synchronously, only the first server will receive them
*/
func TestBalancer_Handle_Sync(t *testing.T) {
	balancer := Balancer{ServersPool: []IServer{
		&mockServer{Url: "0", health: SyncBool{v: true}},
		&mockServer{Url: "1", health: SyncBool{v: true}},
		&mockServer{Url: "2", health: SyncBool{v: true}},
	}}

	r := httptest.NewRequest("GET", "/", nil)

	for i := 0; i < 9; i++ {
		rw := &mockResponseWriter{}
		balancer.Handle(rw, r)
		assert.Equal(t, "0", string(rw.result), "Only the first server should receive requests")
	}
}

/*
Test if connection count is used correctly;
If multiple requests come concurrently (and are processed with an +-EQUAL delay) all servers are used subsequently
*/
func TestBalancer_Handle_Delay(t *testing.T) {
	var wg sync.WaitGroup

	balancer := Balancer{ServersPool: []IServer{
		&mockServer{Url: "0", health: SyncBool{v: true}, delay: 5},
		&mockServer{Url: "1", health: SyncBool{v: true}, delay: 5},
		&mockServer{Url: "2", health: SyncBool{v: true}, delay: 5},
	}}

	wg.Add(3)

	r := httptest.NewRequest("GET", "/", nil)
	cover := [3]bool{false, false, false}

	for i := 0; i < 3; i++ {

		go func(i int) {
			defer wg.Done()
			rw := &mockResponseWriter{}
			balancer.Handle(rw, r)
			idx, err := strconv.Atoi(string(rw.result))
			assert.Nil(t, err)
			cover[idx] = true
		}(i)

	}

	wg.Wait()
	assert.Equal(t, [3]bool{true, true, true}, cover)
}

// Tests if HealthService runs checks in the beginning and correctly identifies faulty servers
func TestBalancer_HealthService_Start(t *testing.T) {
	balancer := Balancer{ServersPool: []IServer{
		&mockServer{Url: "0", health: SyncBool{v: true}},
		&mockServer{Url: "1", health: SyncBool{v: true}, failing: true},
		&mockServer{Url: "2", health: SyncBool{v: true}},
	}}
	balancer.StartHealthService()
	assert.False(t, balancer.ServersPool[1].Health().Get())
}

type mockServer struct {
	Url         string
	connections Counter
	health      SyncBool
	delay       int
	failing     bool
}

func (s *mockServer) Forward(rw http.ResponseWriter, r *http.Request) error {
	s.connections.Add(1)
	//Use milliseconds so tests pass quickly
	if s.delay != 0 {
		time.Sleep(time.Duration(s.delay) * time.Millisecond)
	}
	rw.Write([]byte(s.Url))
	s.connections.Add(-1)
	return nil
}

func (s *mockServer) CheckHealth() {
	if s.failing {
		s.health.Set(false)
	}
}

func (s *mockServer) Connections() *Counter {
	return &s.connections
}

func (s *mockServer) Health() *SyncBool {
	return &s.health
}

type mockResponseWriter struct {
	result []byte
	status int
}

func (rw mockResponseWriter) Header() http.Header {
	return map[string][]string{}
}

func (rw *mockResponseWriter) Write(result []byte) (int, error) {
	rw.result = result
	return len(result), nil
}

func (rw *mockResponseWriter) WriteHeader(status int) {
	rw.status = status
}
