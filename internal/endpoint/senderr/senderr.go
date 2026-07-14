// Package senderr provides the shared retry-classification error type used by
// every outbound-message frontend (Telegram, Discord, ...). A "permanent"
// send failure is one where retrying is guaranteed to reproduce the same
// failure (an oversized message, a missing destination id, a 4xx that isn't
// a rate limit) as opposed to a transient one (network blip, provider
// outage) that background retry can legitimately fix.
//
// This was extracted out of internal/endpoint/telegram once a second
// frontend (Discord) needed the identical type, to avoid two copies of the
// same retry-classification logic silently drifting apart over time.
package senderr

// Permanent marks a Send failure as non-retryable.
type Permanent struct{ Err error }

func (e Permanent) Error() string { return e.Err.Error() }
func (e Permanent) Unwrap() error { return e.Err }
