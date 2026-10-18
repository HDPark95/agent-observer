package normalize

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"agent-observer/internal/model"
)

func browserItem(host string, wrapped bool) model.Record {
	result := model.Record{"content": []any{model.Record{
		"type": "text",
		"text": fmt.Sprintf("Browser tab: 12, Title: \"PRIVATE title\", URL: \"https://%s/PRIVATE?token=SECRET\".\n0 AXWebArea PRIVATE body https://unrelated.test/", host),
	}}}
	if wrapped {
		data, _ := json.Marshal(result)
		result = model.Record{"content": []any{model.Record{"type": "text", "text": string(data)}}}
	}
	return model.Record{
		"type": "McpToolCall", "id": "browser", "server": "cua_repl", "tool": "js",
		"arguments": model.Record{"code": "await tab.getAXState();", "title": "PRIVATE title"},
		"status":    "completed", "result": result,
	}
}

func TestBrowserMetadataKeepsOnlyObservedHostAndAction(t *testing.T) {
	for _, host := range []string{"app.slack.com", "app.clickup.com", "docs.example.test"} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/wrapped=%t", host, wrapped), func(t *testing.T) {
				got := Browser(browserItem(host, wrapped))
				if got.String("label") != host+" · 화면 읽기" {
					t.Fatalf("label = %q", got.String("label"))
				}
				data, err := json.Marshal(got)
				if err != nil {
					t.Fatal(err)
				}
				for _, private := range []string{"PRIVATE", "SECRET", "unrelated"} {
					if strings.Contains(string(data), private) {
						t.Errorf("metadata contains page content or URL details: %s", private)
					}
				}
			})
		}
	}
}

func TestBrowserActionsRequireCallsRatherThanTitlesOrLiterals(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		want       []string
	}{
		{"ordered and deduplicated", "await tab.click(3); await tab.getAXState(); await tab.click(4);", []string{"클릭", "화면 읽기"}},
		{"literal and comments", "const s=\"tab.click()\"; // tab.goto()\n/* tab.scroll() */", []string{}},
		{"title only", "", []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := browserItem("docs.example.test", false)
			item["arguments"] = model.Record{"code": tc.code, "title": "ClickUp 문서 읽기"}
			item["result"] = model.Record{}
			if tc.name == "title only" {
				delete(item.Object("arguments"), "code")
			}
			got := Browser(item)
			if !reflect.DeepEqual(got["browserActions"], tc.want) {
				t.Fatalf("actions = %v, want %v", got["browserActions"], tc.want)
			}
			if len(tc.want) == 0 && got.String("label") != "브라우저 작업" {
				t.Fatalf("unobserved action inferred: %v", got)
			}
		})
	}
	item := browserItem("docs.example.test", false)
	item["server"] = "other"
	if Browser(item) != nil {
		t.Fatal("unrelated MCP tool classified as browser")
	}
}

func TestFailedBrowserCallKeepsToolOwnership(t *testing.T) {
	for _, status := range []string{"failed", "result-error"} {
		t.Run(status, func(t *testing.T) {
			item := browserItem("docs.example.test", false)
			if status == "failed" {
				item["status"] = "failed"
			} else {
				item.Object("result")["isError"] = true
			}
			state := New()
			state.Consume(model.Record{"type": "event_msg", "timestamp": "2026-10-15T00:00:00Z", "payload": model.Record{"type": "task_started", "turn_id": "turn"}})
			state.Consume(model.Record{"type": "event_msg", "timestamp": "2026-10-15T00:00:01Z", "payload": model.Record{"type": "item_completed", "turn_id": "turn", "item": item}})
			node := state.Nodes["browser"]
			if node.String("kind") != "tool" || node.String("parent") != "agent" || node.String("status") != "오류" || node.String("toolName") != "cua_repl.js" {
				t.Fatalf("failed browser node = %v", node)
			}
			data, err := json.Marshal(node)
			if err != nil {
				t.Fatal(err)
			}
			for _, private := range []string{"PRIVATE", "SECRET", "unrelated"} {
				if strings.Contains(string(data), private) {
					t.Fatalf("private browser content entered node: %s", private)
				}
			}
		})
	}
}
