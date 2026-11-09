package authz

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
)

// failingResponseWriter is an http.ResponseWriter whose Write always fails,
// like writing to a client connection that has gone away.
type failingResponseWriter struct {
	header http.Header
	err    error
}

func (f *failingResponseWriter) Header() http.Header {
	if f.header == nil {
		f.header = make(http.Header)
	}
	return f.header
}

func (f *failingResponseWriter) Write([]byte) (int, error) { return 0, f.err }

func (f *failingResponseWriter) WriteHeader(int) {}

// captureKlog routes klog's global logger to an in-memory ktesting logger with
// verbosity 3 enabled, and restores klog when the test finishes. Tests using it
// must not call t.Parallel(), since klog's logger and verbosity are global.
func captureKlog(t *testing.T) ktesting.Buffer {
	t.Helper()

	fs := flag.NewFlagSet("klog", flag.ContinueOnError)
	klog.InitFlags(fs)
	prevVerbosity := fs.Lookup("v").Value.String()
	require.NoError(t, fs.Set("v", "3"))
	require.True(t, klog.V(3).Enabled(), "klog V(3) must be enabled for this test")

	logger := ktesting.NewLogger(t, ktesting.NewConfig(ktesting.Verbosity(3), ktesting.BufferLogs(true)))
	klog.SetLoggerWithOptions(logger)
	t.Cleanup(func() {
		klog.ClearLogger()
		require.NoError(t, fs.Set("v", prevVerbosity))
	})

	underlier, ok := logger.GetSink().(ktesting.Underlier)
	require.True(t, ok)
	return underlier.GetBuffer()
}

func logEntriesOfType(buf ktesting.Buffer, typ ktesting.LogType) []ktesting.LogEntry {
	var entries []ktesting.LogEntry
	for _, entry := range buf.Data() {
		if entry.Type == typ {
			entries = append(entries, entry)
		}
	}
	return entries
}

func newRecordedResponse() *responseRecorder {
	recorder := &responseRecorder{}
	recorder.WriteHeader(http.StatusOK)
	_, _ = recorder.Write([]byte(`{"kind":"Pod"}`))
	return recorder
}

func TestEmitResponseToWriterClientDisconnectIsNotAnError(t *testing.T) {
	buf := captureKlog(t)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // the client has gone away

	recorder := newRecordedResponse()
	recorder.emitResponseToWriter(ctx, &failingResponseWriter{err: errors.New("client connection closed")})

	require.Empty(t, logEntriesOfType(buf, ktesting.LogError), "a client disconnect must not be logged as an error")

	infos := logEntriesOfType(buf, ktesting.LogInfo)
	require.Len(t, infos, 1)
	require.Equal(t, "client disconnected before buffered response was written", infos[0].Message)
	require.Equal(t, 3, infos[0].Verbosity)
	require.Equal(t, []any{
		"status_code", http.StatusOK,
		"body_length", len(recorder.body),
		"err", errors.New("client connection closed"),
	}, infos[0].ParameterKVList)
}

func TestEmitResponseToWriterFailureIsAnError(t *testing.T) {
	buf := captureKlog(t)

	writeErr := errors.New("connection reset")
	recorder := newRecordedResponse()
	recorder.emitResponseToWriter(t.Context(), &failingResponseWriter{err: writeErr})

	require.Empty(t, logEntriesOfType(buf, ktesting.LogInfo))

	errs := logEntriesOfType(buf, ktesting.LogError)
	require.Len(t, errs, 1)
	require.Equal(t, "failed to write response body", errs[0].Message)
	require.ErrorIs(t, errs[0].Err, writeErr)
	require.Equal(t, []any{
		"status_code", http.StatusOK,
		"body_length", len(recorder.body),
	}, errs[0].ParameterKVList)
}
