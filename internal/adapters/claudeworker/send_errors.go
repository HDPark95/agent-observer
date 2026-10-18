package claudeworker

import (
	"encoding/json"

	"agent-observer/internal/workers"
)

func sentMessage(receipt workers.SendReceipt) any { return toolReply(false, receipt) }
func sendFailure(err error) any                   { return toolReply(true, workers.DescribeSendFailure(err)) }
func toolReply(failed bool, value any) any {
	text, _ := json.Marshal(value)
	return map[string]any{"isError": failed, "content": []any{map[string]string{"type": "text", "text": string(text)}}}
}
