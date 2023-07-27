# kube-rebac-proxy

Runs a proxy in front of kube-apiserver that can authorize requests and filter
responses.

Checklist:

- [x] Basic proxy with authentication 
- [ ] Authorize an action with a SpiceDB call
- [ ] Write a relationship based on an incoming kube api call
- [ ] Filter a response with a SpiceDB call
- [ ] Authenticate a request from a ServiceAccount 
- [ ] Add a mutating webhook to intercept all pod creating and switch api endpoints to the proxy
