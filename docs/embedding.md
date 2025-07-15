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
    opts := proxy.NewOptions(proxy.WithEmbeddedProxy, proxy.WithEmbeddedSpiceDBEndpoint)

    // Configure your backend Kubernetes cluster
    opts.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
        // Return your cluster's REST config and transport
        return myClusterConfig, myTransport, nil
    }

    // SpiceDB is already configured for embedded mode via WithEmbeddedSpiceDBEndpoint

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
    // Use functional options to automatically add authentication headers
    embeddedClient := proxySrv.GetEmbeddedClient(
        proxy.WithUser("my-user"),
        proxy.WithGroups("my-group", "admin"),
        proxy.WithExtra("department", "engineering"),
    )

    // Create a Kubernetes client that uses the embedded proxy
    k8sClient := createKubernetesClient(embeddedClient)

    // Use the client normally - all requests go through SpiceDB authorization
    pods, err := k8sClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{})
    if err != nil {
        panic(err)
    }

    fmt.Printf("Found %d pods\n", len(pods.Items))
}

func createKubernetesClient(embeddedClient *http.Client) *kubernetes.Clientset {
    restConfig := rest.CopyConfig(proxy.EmbeddedRestConfig)
    restConfig.Transport = embeddedClient.Transport

    k8sClient, err := kubernetes.NewForConfig(restConfig)
    if err != nil {
        panic(err)
    }

    return k8sClient
}
```

### Configuration Options

## Configuration Options

You can configure the proxy with different combinations of embedded options:

### Full Embedded Mode (Proxy + SpiceDB)
```go
// Both proxy and SpiceDB run embedded
opts := proxy.NewOptions(proxy.WithEmbeddedProxy, proxy.WithEmbeddedSpiceDBEndpoint)
```

### Embedded Proxy with Remote SpiceDB
```go
// Proxy runs embedded, but connects to remote SpiceDB
opts := proxy.NewOptions(proxy.WithEmbeddedProxy)
opts.SpiceDBOptions.SpiceDBEndpoint = "localhost:50051"
opts.SpiceDBOptions.SecureSpiceDBTokensBySpace = "your-token"
```

### Regular Proxy with Embedded SpiceDB
```go
// Proxy runs with TLS termination, but uses embedded SpiceDB
opts := proxy.NewOptions(proxy.WithEmbeddedSpiceDBEndpoint)
```

### Example Configuration
```go
opts := proxy.NewOptions(proxy.WithEmbeddedProxy, proxy.WithEmbeddedSpiceDBEndpoint)

// Backend Kubernetes cluster configuration
opts.RestConfigFunc = func() (*rest.Config, http.RoundTripper, error) {
    // Your cluster configuration
}

// SpiceDB configuration is already set to embedded via WithEmbeddedSpiceDBEndpoint
// For remote SpiceDB, you would instead use:
// opts.SpiceDBOptions.SpiceDBEndpoint = "localhost:50051"
// opts.SpiceDBOptions.SecureSpiceDBTokensBySpace = "your-token"

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
opts := proxy.NewOptions(proxy.WithEmbeddedProxy, proxy.WithEmbeddedSpiceDBEndpoint)

// Configure custom header names
opts.Authentication.Embedded.UsernameHeaders = []string{"Custom-User"}
opts.Authentication.Embedded.GroupHeaders = []string{"Custom-Groups"}
opts.Authentication.Embedded.ExtraHeaderPrefixes = []string{"Custom-Extra-"}

// Complete and create the proxy server
completedConfig, _ := opts.Complete(ctx)
proxySrv, _ := proxy.NewServer(ctx, completedConfig)

// The client will automatically use the custom header names
embeddedClient := proxySrv.GetEmbeddedClient(
    proxy.WithUser("alice"),
    proxy.WithGroups("developers", "admin"),
    proxy.WithExtra("department", "engineering"),
)
// Headers will be: Custom-User: alice, Custom-Groups: developers, etc.
```

This is similar to Kubernetes' request header authentication, but uses a separate dedicated `EmbeddedAuthentication` type for embedded mode and doesn't require client certificate configuration (the requests are trusted because the server is embedded).

### Functional Options for GetEmbeddedClient

The `GetEmbeddedClient()` method supports functional options that automatically add authentication headers based on your configured header names. This eliminates the need to manually add headers to each request:

```go
// Basic client without authentication
client := proxySrv.GetEmbeddedClient()

// Client with user authentication
client := proxySrv.GetEmbeddedClient(
    proxy.WithUser("alice"),
)

// Client with user and groups
client := proxySrv.GetEmbeddedClient(
    proxy.WithUser("alice"),
    proxy.WithGroups("developers", "admin", "reviewers"),
)

// Client with user, groups, and extra attributes
client := proxySrv.GetEmbeddedClient(
    proxy.WithUser("alice"),
    proxy.WithGroups("developers", "admin"),
    proxy.WithExtra("department", "engineering"),
    proxy.WithExtra("team", "platform"),
    proxy.WithExtra("location", "remote"),
)
```

The functional options automatically use the header names you've configured in `opts.Authentication.Embedded`. For example, if you've configured custom header names:

```go
opts.Authentication.Embedded.UsernameHeaders = []string{"My-User"}
opts.Authentication.Embedded.GroupHeaders = []string{"My-Groups"}
opts.Authentication.Embedded.ExtraHeaderPrefixes = []string{"My-Extra-"}

// This client will automatically add:
// My-User: alice
// My-Groups: developers
// My-Groups: admin  
// My-Extra-department: engineering
client := proxySrv.GetEmbeddedClient(
    proxy.WithUser("alice"),
    proxy.WithGroups("developers", "admin"),
    proxy.WithExtra("department", "engineering"),
)
```

Available functional options:
- `WithUser(username string)`: Sets the username
- `WithGroups(groups ...string)`: Sets group memberships  
- `WithExtra(key, value string)`: Sets extra user attributes (can be called multiple times)

This approach provides a clean, type-safe way to configure authentication without manually managing headers.
