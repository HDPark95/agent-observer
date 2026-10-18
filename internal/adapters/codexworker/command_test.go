package codexworker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"agent-observer/internal/workers"
)

func TestFailedTurnReportsCompatibilityWithoutRawProviderText(t *testing.T) {
	for _, tt := range []struct {
		payload      string
		incompatible bool
	}{
		{`{"status":"failed","error":{"message":"The configured model requires a newer version of Codex. PRIVATE_PROVIDER_DETAIL"}}`, true},
		{`{"status":"failed","error":{"message":"PRIVATE_PROVIDER_DETAIL"}}`, false},
		{`{"status":"interrupted"}`, false},
	} {
		var result turn
		if err := json.Unmarshal([]byte(tt.payload), &result); err != nil {
			t.Fatal(err)
		}
		_, err := completedReceipt(workers.Receipt{}, result)
		if err == nil || errors.Is(err, workers.ErrRuntimeUpgradeRequired) != tt.incompatible {
			t.Fatalf("unexpected failure: %v", err)
		}
		if strings.Contains(err.Error(), "PRIVATE_PROVIDER_DETAIL") {
			t.Fatal("provider details leaked")
		}
	}
}

func TestWorkerCommandKeepsCoordinatorCapabilitiesOutOfChildren(t *testing.T) {
	cmd := workerCommand(context.Background(), "/explicit/codex")
	want := []string{"/explicit/codex", "app-server", "--disable", "plugins", "--disable", "multi_agent"}
	if !reflect.DeepEqual(cmd.Args, want) {
		t.Fatalf("worker can inherit coordinator plugin/delegation capabilities: %v", cmd.Args)
	}
}
