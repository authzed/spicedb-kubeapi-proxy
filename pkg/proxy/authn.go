package proxy

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/pflag"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapiserver "k8s.io/apiserver/pkg/server"
	kubeoptions "k8s.io/kubernetes/pkg/kubeapiserver/options"
)

// EmbeddedAuthentication configures authentication for embedded mode
type EmbeddedAuthentication struct {
	Enabled             bool
	UsernameHeaders     []string
	GroupHeaders        []string
	ExtraHeaderPrefixes []string
}

// NewEmbeddedAuthentication creates a new embedded authentication configuration with defaults
func NewEmbeddedAuthentication() *EmbeddedAuthentication {
	return &EmbeddedAuthentication{
		Enabled:             false,
		UsernameHeaders:     []string{"X-Remote-User"},
		GroupHeaders:        []string{"X-Remote-Group"},
		ExtraHeaderPrefixes: []string{"X-Remote-Extra-"},
	}
}

type Authentication struct {
	BuiltInOptions *kubeoptions.BuiltInAuthenticationOptions
	Embedded       EmbeddedAuthentication
}

func NewAuthentication() *Authentication {
	auth := &Authentication{
		BuiltInOptions: kubeoptions.NewBuiltInAuthenticationOptions().
			WithClientCert().
			WithOIDC().
			// TODO: ServiceAccounts
			// WithServiceAccounts().
			WithTokenFile().
			WithRequestHeader(),
		Embedded: *NewEmbeddedAuthentication(),
	}
	// TODO: ServiceAccounts
	// auth.BuiltInOptions.ServiceAccounts.Issuers = []string{"https://spicedb-kubeapi-proxy.default.svc"}
	return auth
}

func (c *Authentication) AdditionalAuthEnabled() bool {
	return c.tokenAuthEnabled() || c.serviceAccountAuthEnabled() || c.oidcAuthEnabled() || c.Embedded.Enabled
}

func (c *Authentication) oidcAuthEnabled() bool {
	return c.BuiltInOptions.OIDC != nil && c.BuiltInOptions.OIDC.IssuerURL != ""
}

func (c *Authentication) tokenAuthEnabled() bool {
	return c.BuiltInOptions.TokenFile != nil && c.BuiltInOptions.TokenFile.TokenFile != ""
}

func (c *Authentication) serviceAccountAuthEnabled() bool {
	return c.BuiltInOptions.ServiceAccounts != nil && len(c.BuiltInOptions.ServiceAccounts.KeyFiles) != 0
}

func (c *Authentication) ApplyTo(ctx context.Context, authenticationInfo *genericapiserver.AuthenticationInfo, servingInfo *genericapiserver.SecureServingInfo) error {
	if c.Embedded.Enabled {
		// For embedded mode, use dedicated embedded authentication configuration
		usernameHeaders := c.Embedded.UsernameHeaders
		groupHeaders := c.Embedded.GroupHeaders
		extraHeaderPrefixes := c.Embedded.ExtraHeaderPrefixes

		authenticationInfo.Authenticator = authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
			// Try username headers in order
			var username string
			for _, header := range usernameHeaders {
				if value := req.Header.Get(header); value != "" {
					username = value
					break
				}
			}
			if username == "" {
				return nil, false, nil
			}

			// Collect groups from all group headers
			var groups []string
			for _, header := range groupHeaders {
				groups = append(groups, req.Header.Values(header)...)
			}

			// Collect extra fields
			extra := make(map[string][]string)
			for key, values := range req.Header {
				for _, prefix := range extraHeaderPrefixes {
					if strings.HasPrefix(key, prefix) {
						extraKey := strings.TrimPrefix(key, prefix)
						// Convert to lowercase as per Kubernetes convention
						extraKey = strings.ToLower(extraKey)
						extra[extraKey] = values
						break
					}
				}
			}

			return &authenticator.Response{
				User: &user.DefaultInfo{
					Name:   username,
					Groups: groups,
					Extra:  extra,
				},
			}, true, nil
		})
		return nil
	}

	authenticatorConfig, err := c.BuiltInOptions.ToAuthenticationConfig()
	if err != nil {
		return err
	}
	if authenticatorConfig.ClientCAContentProvider != nil {
		if err = authenticationInfo.ApplyClientCert(authenticatorConfig.ClientCAContentProvider, servingInfo); err != nil {
			return fmt.Errorf("unable to load client CA file: %w", err)
		}
	}
	if authenticatorConfig.RequestHeaderConfig != nil {
		if err = authenticationInfo.ApplyClientCert(authenticatorConfig.RequestHeaderConfig.CAContentProvider, servingInfo); err != nil {
			return fmt.Errorf("unable to load requestheader CA file: %w", err)
		}
	}

	// TODO: ServiceAccounts

	baseAuthenticator, _, _, _, err := authenticatorConfig.New(ctx)
	if err != nil {
		return err
	}

	authenticationInfo.Authenticator = authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
		resp, ok, err := baseAuthenticator.AuthenticateRequest(req)
		if resp == nil || resp.User == nil {
			return resp, ok, err
		}
		return resp, ok, err
	})

	return nil
}

func (c *Authentication) AddFlags(fs *pflag.FlagSet) {
	c.BuiltInOptions.AddFlags(fs)
}

func (c *Authentication) Validate() []error {
	return nil
}

type Authenticator struct{}
