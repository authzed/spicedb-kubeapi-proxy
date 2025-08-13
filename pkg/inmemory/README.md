# In-Memory HTTP Transport

_note: written as if it will be split into its own package_

A high-performance, zero-network-overhead HTTP transport implementation that bypasses the network layer entirely by calling handlers directly in-process.

## Overview

The `inmemory` package provides an `http.RoundTripper` implementation that directly invokes HTTP handlers in memory during the RoundTrip call, eliminating all network serialization, parsing, and connection overhead.
This is ideal for embedded http services or testing and development environments.

## Quick Start

```go
package main

import (
    "fmt"
    "io"
    "net/http"

    "github.com/authzed/spicedb-kubeapi-proxy/pkg/inmemory"
)

func main() {
    // Create your HTTP handler
    handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(http.StatusOK)
        w.Write([]byte(`{"message": "Hello, World!"}`))
    })

    // Create an HTTP client with in-memory transport
    client := inmemory.NewClient(handler)

    // Make requests - no network involved!
    resp, err := client.Get("http://api.example.com/hello")
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()

    // Headers and status are available immediately
    fmt.Printf("Status: %d\n", resp.StatusCode)
    fmt.Printf("Content-Type: %s\n", resp.Header.Get("Content-Type"))
    
    // Read the response body
    io.Copy(io.Discard, resp.Body)
}
```

## API Reference

### `New(handler http.Handler) *Transport`

Creates a new in-memory transport that will invoke the provided handler directly during RoundTrip execution.

```go
transport := inmemory.New(myHandler)
client := &http.Client{Transport: transport}
```

### `NewClient(handler http.Handler) *http.Client`

Convenience function that creates an HTTP client with an in-memory transport.

```go
client := inmemory.NewClient(myHandler)
```

## Examples

### Basic Usage

```go
handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("Hello!"))
})

client := inmemory.NewClient(handler)
resp, _ := client.Get("http://example.com/")
body, _ := io.ReadAll(resp.Body) // Response contains "Hello!"
```

### With Request Bodies

```go
handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    body, _ := io.ReadAll(r.Body)
    w.Write([]byte(fmt.Sprintf("Echo: %s", body)))
})

client := inmemory.NewClient(handler)
resp, _ := client.Post("http://example.com/echo", "text/plain",
    strings.NewReader("test data"))
body, _ := io.ReadAll(resp.Body) // Response contains "Echo: test data"
```

### Complex Handler

```go
mux := http.NewServeMux()
mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("healthy"))
})
mux.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte(`[{"id": 1, "name": "Alice"}]`))
})

client := inmemory.NewClient(mux)

// Both endpoints work normally
healthResp, _ := client.Get("http://api.com/health")
usersResp, _ := client.Get("http://api.com/api/users")

// Handlers executed immediately, read responses
io.ReadAll(healthResp.Body)
io.ReadAll(usersResp.Body)
```
