package authz

import (
	"bytes"
	"io"
	"sync"
)

// frameCapturingReader wraps an io.ReadCloser and captures bytes read per frame
// this intercepts reading frames from a proto encoded stream, so that we can
// replay the unmodified frame against the client stream if the entry is
// authorized (instead of needing to re-encode the frame).
type frameCapturingReader struct {
	reader         io.ReadCloser
	buffer         *bytes.Buffer
	capturing      bool
	capturedFrames chan []byte
	closed         bool
	mu             sync.Mutex
}

func newFrameCapturingReader(reader io.ReadCloser) *frameCapturingReader {
	return &frameCapturingReader{
		reader:         reader,
		buffer:         &bytes.Buffer{},
		capturedFrames: make(chan []byte, 100), // buffered channel to avoid blocking
	}
}

func (f *frameCapturingReader) Read(p []byte) (n int, err error) {
	n, err = f.reader.Read(p)
	if n > 0 {
		f.mu.Lock()
		if f.capturing {
			f.buffer.Write(p[:n])
		}
		f.mu.Unlock()
	}
	return n, err
}

func (f *frameCapturingReader) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if !f.closed {
		f.closed = true
		close(f.capturedFrames)
	}
	return f.reader.Close()
}

func (f *frameCapturingReader) startCapture() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.capturing = true
	f.buffer.Reset()
}

func (f *frameCapturingReader) finishCapture() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.capturing = false
	captured := make([]byte, f.buffer.Len())
	copy(captured, f.buffer.Bytes())
	f.buffer.Reset()
	return captured
}
