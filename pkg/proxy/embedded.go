package proxy

import (
	"k8s.io/client-go/rest"
)

// EmbeddedRestConfig is the standard REST config for embedded mode.
// This config uses a special "http://embedded" host URL that signals
// to the embedded client transport that requests should be handled
// in-memory rather than over the network.
var EmbeddedRestConfig = &rest.Config{
	Host: EmbeddedProxyHost,
}
