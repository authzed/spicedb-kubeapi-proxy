package proxy

import (
	"context"
	"fmt"
	"net/http"

	"github.com/spf13/pflag"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	genericapiserver "k8s.io/apiserver/pkg/server"
	kubeoptions "k8s.io/kubernetes/pkg/kubeapiserver/options"
)

type Authentication struct {
	BuiltInOptions *kubeoptions.BuiltInAuthenticationOptions
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
	}
	// TODO: ServiceAccounts
	// auth.BuiltInOptions.ServiceAccounts.Issuers = []string{"https://spicedb-kubeapi-proxy.default.svc"}
	return auth
}

func (c *Authentication) AdditionalAuthEnabled() bool {
	return c.tokenAuthEnabled() || c.serviceAccountAuthEnabled() || c.oidcAuthEnabled()
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
