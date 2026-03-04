package distributedtx

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/rest/fake"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

type testRoundTripper struct {
	*testing.T
	expectedPath string
	status       int
}

func (trt testRoundTripper) roundtripper(request *http.Request) (*http.Response, error) {
	require.Equal(trt, trt.expectedPath, request.URL.Path)
	header := http.Header{}
	header.Set("Content-Type", runtime.ContentTypeJSON)
	if request.Body == nil {
		request.Body = http.NoBody
	}
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	resp := &http.Response{
		Header:     header,
		StatusCode: trt.status,
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	return resp, nil
}

func (trt testRoundTripper) toKubeClient() *fake.RESTClient {
	return &fake.RESTClient{
		Client:               fake.CreateHTTPClient(trt.roundtripper),
		NegotiatedSerializer: &serializer.CodecFactory{},
	}
}

func TestWriteToKube(t *testing.T) {
	trt := testRoundTripper{T: t, expectedPath: "/my_way", status: http.StatusCreated}
	ah := ActivityHandler{KubeClient: trt.toKubeClient()}

	resp, err := ah.WriteToKube(t.Context(), &KubeReqInput{
		RequestInfo: &request.RequestInfo{Path: "my_way", Namespace: "ns", Verb: "post"},
		RequestURI:  "/my_way",
		ObjectMeta:  &metav1.ObjectMeta{Name: "my_object_meta"},
		Body:        []byte(`{"hi":"bye"}`),
	})

	require.NoError(t, err)
	require.JSONEq(t, `{"hi":"bye"}`, string(resp.Body))
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

func TestWriteToKubeError(t *testing.T) {
	trt := testRoundTripper{T: t, expectedPath: "/my_way", status: http.StatusInternalServerError}
	ah := ActivityHandler{KubeClient: trt.toKubeClient()}

	resp, err := ah.WriteToKube(t.Context(), &KubeReqInput{
		RequestInfo: &request.RequestInfo{Path: "my_way", Namespace: "ns", Verb: "post"},
		RequestURI:  "/my_way",
		ObjectMeta:  &metav1.ObjectMeta{Name: "my_object_meta"},
		Body:        []byte(`{"hi":"bye"}`),
	})

	require.NoError(t, err)
	require.Nil(t, resp.Body)
	require.Error(t, &resp.Err)
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestCheckKubeResource(t *testing.T) {
	trt := testRoundTripper{T: t, expectedPath: "/a_path/object_name", status: http.StatusOK}
	ah := ActivityHandler{KubeClient: trt.toKubeClient()}

	exists, err := ah.CheckKubeResource(t.Context(), &KubeReqInput{
		RequestInfo: &request.RequestInfo{Path: "a_path", Namespace: "ns1", Verb: "get"},
		RequestURI:  "/a_path",
		ObjectMeta:  &metav1.ObjectMeta{Name: "object_name"},
	})

	require.NoError(t, err)
	require.True(t, exists)
}

func TestCheckKubeResourceError(t *testing.T) {
	trt := testRoundTripper{T: t, expectedPath: "/a_path/object_name", status: http.StatusInternalServerError}
	ah := ActivityHandler{KubeClient: trt.toKubeClient()}

	_, err := ah.CheckKubeResource(t.Context(), &KubeReqInput{
		RequestInfo: &request.RequestInfo{Path: "a_path", Namespace: "ns1", Verb: "get"},
		RequestURI:  "/a_path",
		ObjectMeta:  &metav1.ObjectMeta{Name: "object_name"},
	})

	require.Error(t, err)
}

func TestWriteToKubeDoesNotForwardAcceptEncoding(t *testing.T) {
	// Verify that Accept-Encoding is stripped from forwarded headers.
	// If Accept-Encoding is forwarded to kube, kube may gzip-encode its response,
	// but the REST transport won't auto-decompress (it only decompresses when it
	// added Accept-Encoding itself, setting requestedGzip=true internally).
	// Forwarding Accept-Encoding would cause res.Raw() to return compressed bytes.
	var capturedHeader http.Header
	roundtripper := func(req *http.Request) (*http.Response, error) {
		capturedHeader = req.Header.Clone()
		header := http.Header{}
		header.Set("Content-Type", runtime.ContentTypeJSON)
		return &http.Response{
			Header:     header,
			StatusCode: http.StatusCreated,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"hi":"bye"}`))),
		}, nil
	}
	client := &fake.RESTClient{
		Client:               fake.CreateHTTPClient(roundtripper),
		NegotiatedSerializer: &serializer.CodecFactory{},
	}
	ah := ActivityHandler{KubeClient: client}

	resp, err := ah.WriteToKube(t.Context(), &KubeReqInput{
		RequestInfo: &request.RequestInfo{Path: "my_way", Namespace: "ns", Verb: "post"},
		RequestURI:  "/my_way",
		ObjectMeta:  &metav1.ObjectMeta{Name: "my_object_meta"},
		Body:        []byte(`{"hi":"bye"}`),
		Header: http.Header{
			"Accept-Encoding": []string{"gzip"},
			"Content-Type":    []string{"application/json"},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, capturedHeader)
	require.Empty(t, capturedHeader.Get("Accept-Encoding"),
		"Accept-Encoding must not be forwarded: if kube gzip-encodes the response, "+
			"the REST transport won't decompress it because it didn't set the header itself")
	require.Equal(t, "application/json", capturedHeader.Get("Content-Type"),
		"non-encoding headers should still be forwarded")
	require.JSONEq(t, `{"hi":"bye"}`, string(resp.Body))
}

func TestIdempotencyKey(t *testing.T) {
	payload := &v1.WriteRelationshipsRequest{}
	key, err := idempotencyKeyForPayload(payload, "test-key")
	require.NoError(t, err)
	require.NotNil(t, key)
	require.Equal(t, "test-key", key.Resource.ObjectId)
	require.NotNil(t, key.OptionalExpiresAt)

	sameKey, err := idempotencyKeyForPayload(payload, "test-key")
	require.NoError(t, err)
	key.OptionalExpiresAt = nil
	sameKey.OptionalExpiresAt = nil
	require.True(t, key.EqualMessageVT(sameKey))
}
