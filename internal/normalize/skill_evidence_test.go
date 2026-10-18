package normalize

import (
	"testing"

	"agent-observer/internal/model"
)

func TestCommandExitDoesNotProveIndividualSkillRead(t *testing.T) {
	for _, command := range []string{"cat /skills/demo/SKILL.md", "cat /skills/demo/SKILL.md || true", "false && cat /skills/demo/SKILL.md; true", "cat /skills/demo/SKILL.md /missing"} {
		for _, code := range []any{0, 1, nil} {
			s := New()
			s.Item(model.Record{"item": model.Record{"type": "CommandExecution", "id": "read", "command": []any{"sh", "-c", command}, "exit_code": code, "parsed_cmd": []any{model.Record{"type": "read", "path": "/skills/demo/SKILL.md"}}}}, "2026-10-15T00:00:00Z")
			n := s.Nodes["read:skill:demo"]
			if n.String("status") != "읽기 시도" || n.String("parent") != "read" {
				t.Fatalf("compound exit promoted to read success: %v", n)
			}
		}
	}
}
