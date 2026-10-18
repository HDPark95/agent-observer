package codexworker

import (
	"encoding/json"

	"agent-observer/internal/workers"
)

// sendFailure exposes stable recovery guidance without echoing storage paths,
// provider diagnostics, or message bodies from the underlying error.
func sendFailure(err error) any {
	encoded, _ := json.Marshal(workers.DescribeSendFailure(err))
	return toolResult(false, string(encoded))
}

func failedSend(code string, retryable bool, outcome, action string) any {
	// Retryable means an immediate identical retry is safe and useful. Capacity,
	// authorization, and unavailable-recipient errors need intervention first.
	// These fields contain only fixed strings and a bool, so encoding cannot fail.
	encoded, _ := json.Marshal(struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
		Outcome   string `json:"outcome"`
		Action    string `json:"action"`
	}{code, retryable, outcome, action})
	return toolResult(false, string(encoded))
}
