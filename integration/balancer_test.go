package integration

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

const team = "codebryksy"

func getData(key string) (*http.Response, error) {
	path := fmt.Sprintf("%s/api/v1/some-data", baseAddress)

	// Create a new URL with query parameters
	queryParams := url.Values{}
	queryParams.Set("key", key)
	path += "?" + queryParams.Encode()

	return client.Get(path)
}

/*
Integration environment parameters:
Balancer: timeout = 3 seconds, trace, interval between health checks = 1 second
Server1: initial health = false, delay = 10 milliseconds, health debuggable
Server2: initial health = true, delay = 5 milliseconds, health debuggable
Server3: initial health = true, delay = 5 milliseconds, health debuggable
*/
func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	/*
		HealthService initiates server status, the unhealthy one is ommited
		Synchronous requests lead to usage of only one server - first HEALTHY one
	*/

	for i := 0; i < 5; i++ {
		resp, err := getData(team)
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Error(err)
		}
		assert.Equal(t, "server2:8080", resp.Header.Get("lb-from"))
	}

	// Debug: inverse health of first server (false -> true)
	resp, err := client.Post("http://server1:8080/inverse-health", "", bytes.NewBuffer([]byte{}))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Error(err)
	}

	/*
		After double the max interval server health status is updated and requests go to server1
	*/
	time.Sleep(time.Duration(2) * time.Second)

	resp, err = getData(team)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Error(err)
	}
	assert.Equal(t, "server1:8080", resp.Header.Get("lb-from"))

	// If requests are concurrent, due to delay all servers will be used
	var wg sync.WaitGroup
	wg.Add(6)

	cover := [3]bool{false, false, false}

	for i := 0; i < 6; i++ {
		go func() {
			defer wg.Done()
			resp, err := getData(team)
			if err != nil || resp.StatusCode != http.StatusOK {
				t.Error(err)
			}
			idx := resp.Header.Get("lb-from")[6:7]
			serverIdx, err := strconv.Atoi(idx)
			if err == nil {
				cover[serverIdx-1] = true
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, [3]bool{true, true, true}, cover)

	// Debug: inverse health of first server (true -> false)
	resp, err = client.Post("http://server1:8080/inverse-health", "", bytes.NewBuffer([]byte{}))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Error(err)
	}
	// Debug: inverse health of remaining healthy servers (true -> false)
	resp, err = client.Post("http://server2:8080/inverse-health", "", bytes.NewBuffer([]byte{}))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Error(err)
	}
	resp, err = client.Post("http://server3:8080/inverse-health", "", bytes.NewBuffer([]byte{}))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Error(err)
	}

	/*
		After double the max interval server health status is surely updated
	*/
	time.Sleep(time.Duration(2) * time.Second)

	// If no servers are healthy, requests are failing
	resp, err = getData(team)
	assert.NotEqual(t, http.StatusOK, resp.StatusCode)

	// Clean test state
	for i := 0; i < 3; i++ {
		resp, err = client.Post(fmt.Sprintf("http://server%d:8080/inverse-health", i+1), "", bytes.NewBuffer([]byte{}))
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Error(err)
		}
	}
	time.Sleep(time.Duration(2) * time.Second)
}

func BenchmarkBalancer(b *testing.B) {
	var wg sync.WaitGroup
	wg.Add(b.N)

	for i := 0; i < b.N; i++ {
		go func() {
			defer wg.Done()
			resp, err := getData(team)
			if err != nil || resp.StatusCode != http.StatusOK {
				b.Error(err)
			}
		}()
	}

	wg.Wait()
}
