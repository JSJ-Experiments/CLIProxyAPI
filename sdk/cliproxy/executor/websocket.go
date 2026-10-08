package executor

import (
	"errors"
	"net/http"
)

// UpstreamWebsocketReplayRequiredError indicates that an incremental request
// cannot safely continue because its upstream websocket is no longer reusable.
type UpstreamWebsocketReplayRequiredError struct{}

func (*UpstreamWebsocketReplayRequiredError) Error() string {
	return `{"error":{"message":"upstream transport requires full HTTP replay","type":"server_error","code":"upstream_http_replay_required","status":426}}`
}

func (*UpstreamWebsocketReplayRequiredError) StatusCode() int { return http.StatusUpgradeRequired }

func (*UpstreamWebsocketReplayRequiredError) IsRequestScoped() bool { return true }

// NewUpstreamWebsocketReplayRequiredError creates a request-scoped replay signal.
func NewUpstreamWebsocketReplayRequiredError() error {
	return &UpstreamWebsocketReplayRequiredError{}
}

// IsUpstreamWebsocketReplayRequired reports whether err is the internal replay signal.
func IsUpstreamWebsocketReplayRequired(err error) bool {
	var replayErr *UpstreamWebsocketReplayRequiredError
	return errors.As(err, &replayErr)
}

// UpstreamWebsocketReconnectError asks the downstream to reconnect with a full
// transcript after an established upstream socket loses its credential. The
// original failure stays unwrap-able so quota and retry-after accounting still
// applies to the exhausted credential, not the entire account pool.
// The server must not replay incremental input on another credential: upstream
// response IDs and in-flight tool state are scoped to the original socket.
type UpstreamWebsocketReconnectError struct{ cause error }

func (e *UpstreamWebsocketReconnectError) Error() string { return e.cause.Error() }
func (e *UpstreamWebsocketReconnectError) Unwrap() error { return e.cause }

func NewUpstreamWebsocketReconnectError(cause error) error {
	if cause == nil {
		return nil
	}
	return &UpstreamWebsocketReconnectError{cause: cause}
}

func IsUpstreamWebsocketReconnect(err error) bool {
	var reconnectErr *UpstreamWebsocketReconnectError
	return errors.As(err, &reconnectErr)
}
