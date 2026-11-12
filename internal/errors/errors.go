package apierrors

import (
	"encoding/json"
	"net/http"
)

// APIError is a standardized, Stripe-inspired structured error envelope.
type APIError struct {
	Type      string `json:"type"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Param     string `json:"param,omitempty"`
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id,omitempty"`
}

type errorEnvelope struct {
	Error APIError `json:"error"`
}

// WriteJSON serializes a standardized APIError response with the given HTTP status code.
func WriteJSON(w http.ResponseWriter, status int, err APIError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorEnvelope{Error: err})
}

func Unauthorized(w http.ResponseWriter, reqID, traceID, msg string) {
	WriteJSON(w, http.StatusUnauthorized, APIError{
		Type:      "authentication_error",
		Code:      "invalid_api_key",
		Message:   msg,
		RequestID: reqID,
		TraceID:   traceID,
	})
}

func RateLimited(w http.ResponseWriter, reqID, traceID string, retryAfterSec int) {
	WriteJSON(w, http.StatusTooManyRequests, APIError{
		Type:      "rate_limit_error",
		Code:      "rate_limit_exceeded",
		Message:   "Rate limit exceeded. Please retry after the duration indicated in Retry-After.",
		RequestID: reqID,
		TraceID:   traceID,
	})
	_ = retryAfterSec
}

func BadGateway(w http.ResponseWriter, reqID, traceID, msg string) {
	WriteJSON(w, http.StatusBadGateway, APIError{
		Type:      "api_connection_error",
		Code:      "upstream_unavailable",
		Message:   msg,
		RequestID: reqID,
		TraceID:   traceID,
	})
}

func NotFound(w http.ResponseWriter, reqID, traceID, path string) {
	WriteJSON(w, http.StatusNotFound, APIError{
		Type:      "invalid_request_error",
		Code:      "route_not_found",
		Message:   "No upstream route configured for path: " + path,
		Param:     "path",
		RequestID: reqID,
		TraceID:   traceID,
	})
}
