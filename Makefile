.PHONY: build test check lint fmt-check fmt lint-tools plugin
.DEFAULT_GOAL := build

PLUGIN_OUTPUT ?= dist/codex-plugin

plugin:
	python3 scripts/build_plugin.py --output "$(PLUGIN_OUTPUT)"

STATICCHECK_VERSION := v0.7.0
STATICCHECK := $(CURDIR)/bin/tools/staticcheck-$(STATICCHECK_VERSION)
GO_SOURCES := $(shell find cmd internal agent_observer checks -type f -name '*.go')

build:
	go build -trimpath -o bin/agent-observer ./cmd/agent-observer

check: lint test

lint: fmt-check $(STATICCHECK)
	go vet ./...
	$(STATICCHECK) ./...

fmt-check:
	@files="$$(gofmt -l $(GO_SOURCES))" || exit 1; \
	if [ -n "$$files" ]; then printf '%s\n' "$$files"; exit 1; fi

fmt:
	gofmt -w $(GO_SOURCES)

lint-tools: $(STATICCHECK)

$(STATICCHECK):
	mkdir -p "$(CURDIR)/bin/tools"
	GOBIN="$(CURDIR)/bin/tools" go install honnef.co/go/tools/cmd/staticcheck@$(STATICCHECK_VERSION)
	mv "$(CURDIR)/bin/tools/staticcheck" "$@"

test:
	go test -race ./...
	go vet ./...
	python3 -m unittest discover -s tests -v
	node checks/initial_render.cjs
