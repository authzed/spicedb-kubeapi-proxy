# Embedding

The SpiceDB KubeAPI Proxy supports an embedded mode that allows you to integrate the proxy directly into your application without requiring TLS certificates or binding to network ports. 
Under the hood, it uses the [`pkg/inmemory`](./pkg/inmemory/) transport package for zero-overhead HTTP communication.

## Usage

### Basic Setup

```go
package main

import (
    "context"
    "net/http"

    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"

    "github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy"
)

func main() {
    ctx := context.Background()

    // Create options with embedded mode enabled
    opts := proxy.NewOptions()
    opts.EmbeddedMode = true

    // Configure your backend Kubernetes cluster
    opts.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
        // Return your cluster's REST config and transport
        return myClusterConfig, myTransport, nil
    }

    // Configure SpiceDB (can use embedded SpiceDB too)
    opts.SpiceDBOptions.SpiceDBEndpoint = "embedded://"

    // Set up your authorization rules
    opts.RuleConfigFile = "rules.yaml"

    // Complete configuration
    if err := opts.Complete(ctx); err != nil {
        panic(err)
    }

    // Create the proxy server
    proxySrv, err := proxy.NewServer(ctx, *opts)
    if err != nil {
        panic(err)
    }

    // Get an HTTP client that connects directly to the embedded proxy
    embeddedClient := proxySrv.GetEmbeddedClient()

    // Create a Kubernetes client that uses the embedded proxy
    k8sClient := createKubernetesClient(embeddedClient, "my-user", []string{"my-group"})

    // Use the client normally - all requests go through SpiceDB authorization
    pods, err := k8sClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
    if err != nil {
        panic(err)
    }

    fmt.Printf("Found %d pods\n", len(pods.Items))
}

func createKubernetesClient(embeddedClient *http.Client, username string, groups []string) *kubernetes.Clientset {
    restConfig := &rest.Config{
        Host:      "http://embedded", // Special URL for embedded mode
        Transport: embeddedClient.Transport,
    }

    // Add authentication headers
    restConfig.WrapTransport = func(rt http.RoundTripper) http.RoundTripper {
        return &authTransport{
            username: username,
            groups:   groups,
            rt:       rt,
        }
    }

    k8sClient, err := kubernetes.NewForConfig(restConfig)
    if err != nil {
        panic(err)
    }

    return k8sClient
}

type authTransport struct {
    username string
    groups   []string
    rt       http.RoundTripper
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
    // Add authentication headers (using default header names)
    // These can be customized using --embedded-* flags
    req.Header.Set("X-Remote-User", t.username)
    for _, group := range t.groups {
        req.Header.Add("X-Remote-Group", group)
    }
    return t.base.RoundTrip(req)
}
```

### Configuration Options

When using embedded mode, you can set these options:

```go
opts := proxy.NewOptions()

// Enable embedded mode
opts.EmbeddedMode = true

// Backend Kubernetes cluster configuration
opts.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
    // Your cluster configuration
}

// SpiceDB configuration (can use embedded SpiceDB)
opts.SpiceDBOptions.SpiceDBEndpoint = "embedded://"  // or "localhost:50051"
opts.SpiceDBOptions.SecureSpiceDBTokensBySpace = "your-token"

// Authorization rules
opts.RuleConfigFile = "path/to/rules.yaml"
```

### Authentication Headers

In embedded mode, authentication is handled via HTTP headers:

The embedded proxy has a dedicated `EmbeddedAuthentication` configuration that is designed for programmatic use only. When embedding the proxy in your Go application, you can configure the header names through the `opts.Authentication.Embedded` struct:

- `opts.Authentication.Embedded.UsernameHeaders`
- `opts.Authentication.Embedded.GroupHeaders`
- `opts.Authentication.Embedded.ExtraHeaderPrefixes`

**Default Headers:**
- `X-Remote-User`: The username (required)
- `X-Remote-Group`: Group membership (can be specified multiple times)
- `X-Remote-Extra-*`: Extra user attributes (e.g., `X-Remote-Extra-Department: engineering`)

**Example with default headers:**

```
X-Remote-User: alice
X-Remote-Group: developers
X-Remote-Group: admin
X-Remote-Extra-Department: engineering
X-Remote-Extra-Team: platform
```

**Example with custom headers (programmatic configuration):**

```go
opts := proxy.NewOptions()
opts.EmbeddedMode = true

// Configure custom header names
opts.Authentication.Embedded.UsernameHeaders = []string{"Custom-User"}
opts.Authentication.Embedded.GroupHeaders = []string{"Custom-Groups"}
opts.Authentication.Embedded.ExtraHeaderPrefixes = []string{"Custom-Extra-"}
```

Then use custom headers in requests:
```
Custom-User: alice
Custom-Groups: developers
Custom-Groups: admin
Custom-Extra-Department: engineering
```

This is similar to Kubernetes' request header authentication, but uses a separate dedicated `EmbeddedAuthentication` type for embedded mode and doesn't require client certificate configuration (the requests are trusted because the server is embedded).
