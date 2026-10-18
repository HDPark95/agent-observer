package normalize

import (
	"path/filepath"
	"strings"

	"agent-observer/internal/model"
)

// simpleCommandWords declines shell control and expansion. A mixed command must
// not become an internal observation label that hides its actual product work.
func simpleCommandWords(source string) []string {
	var quote rune
	escaped := false
	for _, c := range strings.TrimSpace(source) {
		if escaped {
			if c == '\n' {
				return nil
			}
			escaped = false
			continue
		}
		if c == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != '\'' && (c == '$' || c == '`') {
			return nil
		}
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
		} else if strings.ContainsRune(";&|()<>\n#", c) {
			return nil
		}
	}
	return shellWords(source)
}

func unwrapUV(words []string) []string {
	if len(words) < 2 || filepath.Base(words[0]) != "uv" || words[1] != "run" {
		return words
	}
	words = words[2:]
	for len(words) > 0 && (words[0] == "--offline" || words[0] == "--no-sync") {
		words = words[1:]
	}
	return words
}

// mailboxStdinCommand recognizes the CLI's body-file input without accepting
// redirection for internal observation commands or interpreting the file body.
func mailboxStdinCommand(source string) []string {
	var quote rune
	escaped := false
	for index, char := range source {
		if escaped {
			escaped = false
			continue
		}
		if char == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if char == quote {
				quote = 0
			}
			continue
		}
		if char == '\'' || char == '"' {
			quote = char
			continue
		}
		if char != '<' {
			continue
		}
		command := simpleCommandWords(source[:index])
		input := simpleCommandWords(source[index+1:])
		if len(command) < 4 || filepath.Base(command[0]) != "agent-observer" || command[1] != "message" || command[2] != "send" || !has(command[3:], "--body-stdin") || len(input) != 1 {
			return nil
		}
		return command
	}
	return nil
}

func taskLabel(words []string) string {
	w := unwrapUV(words)
	if len(w) == 0 {
		return ""
	}
	exe, rest := filepath.Base(w[0]), w[1:]
	if (exe == "python" || exe == "python3") && len(rest) > 1 && rest[0] == "-m" {
		return map[string]string{"unittest": "테스트 실행", "pytest": "테스트 실행", "checks.reset_contract": "기록 초기화 동작 검사"}[rest[1]]
	}
	if exe == "pytest" && !has(rest, "--version") && !has(rest, "--help") {
		return "테스트 실행"
	}
	if exe == "go" && len(rest) > 0 && rest[0] == "test" {
		return goTestLabel(rest[1:])
	}
	if exe == "agent-observer" && len(rest) > 1 && rest[0] == "message" {
		return map[string]string{"send": "메시지 보내기", "receive": "메시지 읽기", "ack": "메시지 수신 확인", "enroll": "에이전트 메시지 연결"}[rest[1]]
	}
	if exe == "node" && len(rest) > 0 {
		if rest[0] == "checks/initial_render.cjs" || strings.HasSuffix(rest[0], "/checks/initial_render.cjs") {
			return "화면 동작 검사"
		}
		if rest[0] == "--test" {
			return "테스트 실행"
		}
	}
	if exe == "npm" || exe == "pnpm" || exe == "yarn" {
		if len(rest) > 0 && rest[0] == "run" {
			rest = rest[1:]
		}
		if len(rest) > 0 {
			return map[string]string{"test": "테스트 실행", "build": "빌드 실행", "lint": "코드 스타일 검사", "typecheck": "타입 검사"}[rest[0]]
		}
	}
	return ""
}

// goTestLabel uses explicit package and test selectors, never test arguments.
func goTestLabel(args []string) string {
	packages, messagePackages := 0, 0
	selection := ""
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-args" {
			break
		}
		flag, value, inline := strings.Cut(arg, "=")
		if flag == "-run" {
			if !inline && i+1 < len(args) {
				i++
				value = args[i]
			}
			selection = value
			continue
		}
		if strings.HasPrefix(arg, "-") {
			if !inline && has([]string{"-bench", "-benchtime", "-blockprofile", "-blockprofilerate", "-count", "-covermode", "-coverpkg", "-coverprofile", "-cpu", "-cpuprofile", "-exec", "-fuzz", "-fuzzminimizetime", "-fuzztime", "-list", "-memprofile", "-memprofilerate", "-mod", "-modfile", "-mutexprofile", "-mutexprofilefraction", "-o", "-outputdir", "-overlay", "-p", "-parallel", "-shuffle", "-skip", "-tags", "-timeout", "-trace", "-vet"}, flag) {
				i++
			}
			continue
		}
		packages++
		pkg := strings.TrimPrefix(strings.TrimSuffix(arg, "/..."), "./")
		pkg = strings.TrimPrefix(pkg, "agent-observer/")
		if has([]string{"internal/messaging", "internal/messagecli", "internal/messages"}, pkg) {
			messagePackages++
		}
	}
	if strings.Contains(selection, "SSE") || strings.Contains(selection, "Stream") {
		return "SSE 테스트 실행"
	}
	if packages > 0 && packages == messagePackages {
		return "메시지 테스트 실행"
	}
	return "Go 테스트 실행"
}

// observationLabel requires the Python module at its actual execution position.
func observationLabel(words []string) (string, string) {
	w := unwrapUV(words)
	if len(w) < 3 || !has([]string{"python", "python3"}, filepath.Base(w[0])) || w[1] != "-m" {
		return "", ""
	}
	module := w[2]
	if !has([]string{"tools.skill_observation", "tools.observation_workflow", "tools.task_observation", "tools.task_measurement"}, module) {
		return "", ""
	}
	action := "실행"
	if len(w) > 3 {
		if known := map[string]string{"start": "시작", "read": "읽기 기록", "end": "종료", "finish": "마감", "deliver": "전송", "resume": "재개", "lookup": "조회 기록", "agent": "에이전트 기록", "mark": "진행 기록", "question": "질문 기록", "answer": "응답 기록", "rework": "재작업 기록", "summary": "요약"}[w[3]]; known != "" {
			action = known
		}
	}
	prefix := "작업 관찰 · "
	if module == "tools.skill_observation" {
		prefix = "스킬 관찰 · "
	}
	return prefix + action, module
}

func documentLabel(reads []model.Record) string {
	label := ""
	for _, read := range reads {
		name := read.String("path")
		if name == "" {
			name = read.String("name")
		}
		current := map[string]string{
			"messaging.md": "메시지 문서 검토", "messaging-design-proposal.md": "메시지 문서 검토",
			"harness-observability-usage.md": "하네스 관측 문서 읽기", "skill-observation-usage.md": "하네스 관측 문서 읽기", "task-friction-usage.md": "하네스 관측 문서 읽기",
		}[filepath.Base(name)]
		if current == "" || (label != "" && current != label) {
			return ""
		}
		label = current
	}
	return label
}
