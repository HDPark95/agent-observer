package normalize

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"

	"agent-observer/internal/model"
)

var operations = map[string]string{"createBrowserTab": "페이지 열기", "goto": "페이지 이동", "getTab": "탭 연결", "reload": "새로고침", "back": "뒤로 이동", "forward": "앞으로 이동", "getAXState": "화면 읽기", "getAXStateAndScreenshot": "화면 읽기", "domSnapshot": "화면 읽기", "getScreenshot": "화면 캡처", "screenshot": "화면 캡처", "click": "클릭", "dblclick": "클릭", "scroll": "스크롤", "fill": "입력", "setValue": "입력", "typeText": "입력", "pressKey": "키 입력", "getState": "브라우저 목록 확인", "listTabs": "탭 목록 확인", "listBrowsers": "브라우저 목록 확인"}
var literals *regexp.Regexp

func init() {
	literals = regexp.MustCompile("'(?:\\\\.|[^'\\\\])*'|\"(?:\\\\.|[^\"\\\\])*\"|`(?:\\\\.|[^`\\\\])*`|/\\*[\\s\\S]*?\\*/|//[^\\n]*")
}

var methods = regexp.MustCompile(`\.\s*([A-Za-z]+)\s*\(`)
var tabHeader = regexp.MustCompile(`(?m)^Browser tab: [^\n]+?, Title: [^\n]*?, URL: "([^"\n]+)"\.?$`)
var hostPattern = regexp.MustCompile(`^[A-Za-z0-9.-]{1,253}$`)

func addUnique(list []string, s string) []string {
	if s != "" && !has(list, s) {
		return append(list, s)
	}
	return list
}
func browserTexts(r model.Record, depth int) []string {
	out := []string{}
	if depth > 3 {
		return out
	}
	for _, v := range r.Array("content") {
		b := model.As(v)
		if b.String("type") != "text" {
			continue
		}
		s, ok := b["text"].(string)
		if !ok {
			continue
		}
		var wrapped model.Record
		if json.Unmarshal([]byte(s), &wrapped) == nil && wrapped.Array("content") != nil {
			out = append(out, browserTexts(wrapped, depth+1)...)
		} else {
			out = append(out, s)
		}
	}
	return out
}
func Browser(item model.Record) model.Record {
	if item.String("server") != "cua_repl" || item.String("tool") != "js" {
		return nil
	}
	actions := []string{}
	for _, m := range methods.FindAllStringSubmatch(literals.ReplaceAllString(item.Object("arguments").String("code"), " "), -1) {
		actions = addUnique(actions, operations[m[1]])
	}
	sites := []string{}
	for _, text := range browserTexts(item.Object("result"), 0) {
		for _, m := range tabHeader.FindAllStringSubmatch(text, -1) {
			u, e := url.Parse(m[1])
			if e == nil && (u.Scheme == "http" || u.Scheme == "https") && hostPattern.MatchString(u.Hostname()) {
				sites = addUnique(sites, strings.ToLower(u.Hostname()))
			}
		}
	}
	if len(sites) > 3 {
		sites = sites[:3]
	}
	limit := len(actions)
	if limit > 3 {
		limit = 3
	}
	label := strings.Join(actions[:limit], " · ")
	if label == "" {
		label = "브라우저 작업"
	}
	if len(sites) > 0 {
		label = strings.Join(sites, " / ") + " · " + label
	}
	return model.Record{"label": label, "toolName": "cua_repl.js", "browserActions": actions, "browserSites": sites, "evidence": "브라우저 호출 메서드 및 반환된 탭 헤더 기반 분류", "scope": "행동 분류는 추정이며 읽기·클릭의 성공이나 문서 이해를 뜻하지 않습니다. 페이지 제목·본문·URL 경로·입력값은 저장하지 않습니다."}
}
