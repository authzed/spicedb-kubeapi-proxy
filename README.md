# spicedb-kubeapi-proxy

<a href="https://app.codecov.io/gh/authzed/spicedb-kubeapi-proxy"><img alt="coverage" src="https://img.shields.io/codecov/c/github/authzed/spicedb-kubeapi-proxy"></a>

`spicedb-kubeapi-proxy` is a proxy that runs in between clients and the kube
apiserver that can authorize requests and filter responses using an embedded or
remote SpiceDB.

## Status

The [issues](https://github.com/authzed/spicedb-kubeapi-proxy/issues) track
the current state of the project, but the primary goals before 1.0 are:

- Stabilizing the API for configuring proxy rules
- Gaining operational experience and proving the system in production

## Features

- 🚀 Authorize any request to the Kubernetes cluster based on data in SpiceDB
- ✨ Filter responses (including lists) from the kubernetes cluster based on data in SpiceDB
- 🌶️ Write to both SpiceDB and Kubernetes in a single transaction (durably)
- 🪩 Use different user authentication in the proxy than you do in the base cluster
- 🎉 No syncing between SpiceDB and Kubernetes is required
- 🔒 Does not require admin permissions in the base cluster
- 📦 Run the proxy in-cluster or out-of-cluster
- 📡 Use an embedded SpiceDB or a remote SpiceDB
- 📜 Configure with a variety of different rules to control access to the cluster
- 📊 Metrics and tracing support

## Architecture

![Arch Diagram Dark](./docs/proxy-arch-dark.png#gh-dark-mode-only)![Arch Diagram Light](./docs/proxy-arch-light.png#gh-light-mode-only)

The proxy authenticates itself with the downstream kube-apiserver (client certs
if running out-of-cluster, service account token if running in-cluster).
The proxy is configured with a set of rules that define how to authorize requests
and how to filter responses by communicating with SpiceDB.

There are three basic types of rule:

- **Check** rules: these are used to authorize whether a request is allowed to
  proceed at all. For example, a rule might say that a user can only list pods
  in a namespace `foo` if they have a `namespace:foo#list@user:alice` permission
  in SpiceDB.
- **Filter** rules: these are used to filter the response from the kube-apiserver
  based on the data in SpiceDB. For example, a rule might say that a user can
  only see the pods in namespace `foo` if there are corresponding relationships
  in SpiceDB that enumerate the allowed pods, like `pod:foo/a#view@user:alice`
  and `pod:foo/b#view@user:alice`. In this example, `alice` would see pods `a`
  and `b` in namespace `foo`, but no others.
- **Update Relationship** rules: these are used to write/delete data in
  SpiceDB based on the request that the proxy is authorizing. For example,
  if `alice` creates a new pod `c` in namespace `foo`, a rule can determine
  that a relationship should be written to SpiceDB that grants ownership,
  i.e. `pod:foo/a#view@user:alice`. Rules support both single relationship
  templates and **tupleSet** expressions that can generate multiple relationships
  dynamically based on resource content (e.g., one relationship per container
  in a Deployment).

Rules often work in tendem; for example, a `Check` rule might authorize a request
to list pods in a namespace, and a `Filter` rule might further restrict the
response to only include certain pods.

Note that the proxy does not assume anything about the structure of the data in
SpiceDB. It is up to the user to define the data in SpiceDB and the rules that
the proxy uses to authorize and filter requests.

The proxy rejects any request for which it doesn't find a matching rule.

# Development

This project uses `mage` to offer various development-related commands.

```bash
# run to get all available commands
brew install mage
mage
```

## Tests

Runs both unit and e2e tests

```bash
mage test:all
```

## Development environment

```bash
mage dev:up
```

This brings a development kind cluster with the proxy running in it with an embedded SpiceDB.
A development `dev.kubeconfig` file will be generated so that you can configure your client
to talk to either the proxy or the upstream kind cluster.

Examples:

```bash
kubectl --kubeconfig $(pwd)/dev.kubeconfig --context proxy get namespace
```

or

```bash
export KUBECONFIG=$(pwd)/dev.kubeconfig
kubectx proxy
kubectl get namespace
```

You can also talk to the upstream cluster to verify things by switching to the context name `admin`:

```bash
kubectl --kubeconfig $(pwd)/dev.kubeconfig --context admin get namespace
```

To clean everything up just run:

```bash
mage dev:clean
```

## Run the proxy locally

Sometimes you may want to debug the proxy. The easiest way would be to spin up the development environment with `mage dev:up`
and then run the proxy targeting it as upstream:

```bash
mage dev:run
```

Alternatively if you want to run with delve or your IDE, do:

```bash
go run ./cmd/spicedb-kubeapi-proxy/main.go --bind-address=127.0.0.1 --secure-port=8443 --backend-kubeconfig $(pwd)/spicedb-kubeapi-proxy.kubeconfig --client-ca-file $(pwd)/client-ca.crt --requestheader-client-ca-file $(pwd)/client-ca.crt --spicedb-endpoint embedded://
```

You'll then be able to reach out to your local proxy instance with the context `local`. Note TLS certs are
auto-generated by Kube so `--insecure-skip-tls-verify` must be provided.

```bash
export KUBECONFIG=$(pwd)/dev.kubeconfig
kubectx proxy
kubectl --insecure-skip-tls-verify get namespace
```
