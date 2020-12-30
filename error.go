package steadyrabbit

import "errors"

// errors
var (
	ErrConnectionClosed = errors.New("connection has been closed")
	ErrConnectionDial     = errors.New("connection dial error")
)
