package normalize

import (
	"strings"
	"testing"

	"agent-observer/internal/model"
)

func TestCommandClassificationPriority(t *testing.T) {
	for _, tc := range []struct {
		name, command string
		parsed        []any
		label, tool   string
	}{
		{
			name: "skill read precedes test command", command: "python3 -m unittest",
			parsed: []any{model.Record{"type": "read", "path": "/skills/go-style/SKILL.md"}},
			label:  "스킬 읽기 · go-style", tool: "파일 읽기",
		},
		{
			name: "test command precedes generic read", command: "uv run --offline --no-sync python3 -m unittest",
			parsed: []any{model.Record{"type": "read", "path": "/project/check.py"}},
			label:  "테스트 실행", tool: "uv",
		},
		{
			name: "observation precedes generic read", command: "python3 -m tools.skill_observation read --run-id private-value",
			parsed: []any{model.Record{"type": "read", "path": "/project/check.py"}},
			label:  "스킬 관찰 · 읽기 기록", tool: "tools.skill_observation",
		},
		{
			name: "unknown observation action", command: "python3 -m tools.observation_workflow unknown",
			label: "작업 관찰 · 실행", tool: "tools.observation_workflow",
		},
		{
			name: "read summary precedes executable", command: "cat /private/project/first.go /private/project/second.go",
			parsed: []any{model.Record{"type": "read", "path": "/private/project/first.go"}, model.Record{"type": "read", "path": "/private/project/second.go"}},
			label:  "파일 읽기 · first.go 외 1개", tool: "cat",
		},
		{
			name: "parsed read name fallback", command: "head file",
			parsed: []any{model.Record{"type": "read", "name": "note.md"}},
			label:  "파일 읽기 · note.md", tool: "head",
		},
		{name: "file search", command: "rg --files", label: "파일 목록 검색 · rg", tool: "rg"},
		{name: "script basename", command: "python3 /private/project/verify.py --token hidden", label: "스크립트 실행 · verify.py", tool: "python3"},
		{name: "unrecognized command", command: "mystery --token hidden", label: "셸 명령 실행", tool: "shell"},
		{name: "harness identity", command: "uv run --offline --no-sync python -m tools.task_observation agent --run-id PRIVATE", label: "작업 관찰 · 에이전트 기록", tool: "tools.task_observation"},
		{name: "harness measurement", command: ".venv/bin/python -m tools.task_measurement rework --reason PRIVATE", label: "작업 관찰 · 재작업 기록", tool: "tools.task_measurement"},
		{name: "harness action is positional", command: "python3 -m tools.task_measurement unknown --reason start", label: "작업 관찰 · 실행", tool: "tools.task_measurement"},
		{name: "go tests", command: "go test -race ./...", label: "Go 테스트 실행", tool: "go"},
		{name: "sse tests", command: "go test ./internal/httpapi -run 'TestStreamFilesystemReconnectAndIsolation|TestMissingDiscoveryDirectory' -count=1", label: "SSE 테스트 실행", tool: "go"},
		{name: "inline sse selection", command: "go test ./internal/httpapi -run=TestMailboxSSEWithoutTranscriptWrite", label: "SSE 테스트 실행", tool: "go"},
		{name: "messaging tests", command: "go test -race ./internal/messaging ./internal/messagecli ./internal/messages", label: "메시지 테스트 실행", tool: "go"},
		{name: "mixed tests", command: "go test ./internal/messaging ./internal/httpapi", label: "Go 테스트 실행", tool: "go"},
		{name: "test arguments are not selectors", command: "go test ./... -args -run TestStreamPrivate", label: "Go 테스트 실행", tool: "go"},
		{name: "last selector wins", command: "go test ./... -run TestStreamPrivate -run TestOther", label: "Go 테스트 실행", tool: "go"},
		{name: "output path is not package", command: "go test -coverprofile ./internal/messages ./...", label: "Go 테스트 실행", tool: "go"},
		{name: "message send", command: "./bin/agent-observer message send --body-stdin --message-id PRIVATE < /tmp/private-message.txt", label: "메시지 보내기", tool: "agent-observer"},
		{name: "attached stdin redirection", command: "./bin/agent-observer message send --body-stdin<'/tmp/PRIVATE; message.txt'", label: "메시지 보내기", tool: "agent-observer"},
		{name: "message receive", command: "./bin/agent-observer message receive --wait 1s", label: "메시지 읽기", tool: "agent-observer"},
		{name: "message acknowledge", command: "./bin/agent-observer message ack --message-id PRIVATE", label: "메시지 수신 확인", tool: "agent-observer"},
		{name: "message enrollment", command: "./bin/agent-observer message enroll --token PRIVATE", label: "에이전트 메시지 연결", tool: "agent-observer"},
		{
			name: "message documentation", command: "cat docs/messaging.md",
			parsed: []any{model.Record{"type": "read", "path": "/project/docs/messaging.md"}},
			label:  "메시지 문서 검토", tool: "cat",
		},
		{
			name: "observation documentation", command: "cat docs/skill-observation-usage.md",
			parsed: []any{model.Record{"type": "read", "path": "/harness/docs/skill-observation-usage.md"}},
			label:  "하네스 관측 문서 읽기", tool: "cat",
		},
		{
			name: "mixed documentation stays visible", command: "cat docs/skill-observation-usage.md docs/product.md",
			parsed: []any{model.Record{"type": "read", "path": "/harness/docs/skill-observation-usage.md"}, model.Record{"type": "read", "path": "/project/docs/product.md"}},
			label:  "파일 읽기 · skill-observation-usage.md 외 1개", tool: "cat",
		},
		{
			name: "compound document read stays visible", command: "cat docs/skill-observation-usage.md; go test ./...",
			parsed: []any{model.Record{"type": "read", "path": "/harness/docs/skill-observation-usage.md"}},
			label:  "파일 읽기 · skill-observation-usage.md", tool: "cat",
		},
		{
			name: "mixed parsed actions stay visible", command: "cat docs/skill-observation-usage.md",
			parsed: []any{model.Record{"type": "read", "path": "/harness/docs/skill-observation-usage.md"}, model.Record{"type": "unknown"}},
			label:  "파일 읽기 · skill-observation-usage.md", tool: "cat",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := model.Record{"command": []any{"sh", "-c", tc.command}, "parsed_cmd": tc.parsed}
			label, tool := Command(item)
			if label != tc.label || tool != tc.tool {
				t.Errorf("Command() = (%q, %q), want (%q, %q)", label, tool, tc.label, tc.tool)
			}
		})
	}
}

func TestMailboxLabelDoesNotAcceptCompoundOrSubstitution(t *testing.T) {
	for _, command := range []string{
		`./bin/agent-observer message send --body-stdin < /tmp/body; go test ./...`,
		`./bin/agent-observer message send --body-stdin < $(cat /tmp/path)`,
		"./bin/agent-observer message send --body-stdin <<EOF\nPRIVATE\nEOF",
		`./bin/agent-observer message send --body-stdin < /tmp/body | cat`,
	} {
		t.Run(command, func(t *testing.T) {
			label, _ := Command(model.Record{"command": []any{"sh", "-c", command}})
			if label == "메시지 보내기" {
				t.Errorf("compound command classified as a single message send")
			}
		})
	}
}

func TestObservationLabelsRequireSingleExecutedModule(t *testing.T) {
	for _, command := range []string{
		`echo -m tools.task_observation start`,
		`echo "python3 -m tools.skill_observation start"`,
		`python3 -c "print('-m tools.observation_workflow start')"`,
		`python3 -m tools.skill_observation start;go test ./...`,
		`python3 -m tools.task_measurement start && go test ./...`,
		"python3 -m tools.task_observation start\ngo test ./...",
		`python3 -m tools.skill_observation start --run-id "$(go test ./...)"`,
		`python3 -m tools.skill_observation start --run-id ` + "`go test ./...`",
		`uv run --offline python3 -m tools.task_observation start | cat`,
	} {
		t.Run(command, func(t *testing.T) {
			label, tool := Command(model.Record{"command": []any{"sh", "-c", command}})
			if strings.Contains(label, "관찰") || strings.HasPrefix(tool, "tools.") {
				t.Errorf("non-observation command classified as internal: (%q, %q)", label, tool)
			}
		})
	}
}
