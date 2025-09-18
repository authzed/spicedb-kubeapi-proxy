package authz

import (
	"net/http"
	"strings"
)

func requestLogger(r *http.Request) string {
	var sb strings.Builder

	sb.WriteString(r.Method)
	sb.WriteString(" ")
	sb.WriteString(r.URL.String())

	var bodyBytes []byte
	if r.Body != nil {
		defer func() {
			_ = r.Body.Close()
		}()
		bodyBytes = make([]byte, r.ContentLength)
		_, err := r.Body.Read(bodyBytes)
		if err != nil {
			sb.WriteString("Body: <unknown>")
		} else {
			sb.WriteString(" Body: " + string(bodyBytes))
		}
	}

	return sb.String()
}
