package delivery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"agent-observer/internal/domain"
	"agent-observer/internal/projection"
	"agent-observer/internal/sources"
)

// SourceCatalog supplies metadata only for explicitly selected sessions.
type SourceCatalog interface{ SelectedSources() []sources.Selection }

type collection struct {
	reader       sources.History
	pending      []domain.Observation
	lastRevision string
}

// Collector owns export cursors independently of browser snapshots.
type Collector struct {
	source    SourceCatalog
	box       *Outbox
	readers   map[string]*collection
	mu        sync.Mutex
	lastError string
	warning   string
}

func NewCollector(source SourceCatalog, box *Outbox) *Collector {
	return &Collector{source: source, box: box, readers: map[string]*collection{}}
}

// Run collects every retained turn, independently of open browser tabs. The
// one-second export cadence does not change filesystem-driven UI SSE delivery.
func (c *Collector) Run(ctx context.Context) {
	done := make(chan struct{})
	go func() { c.box.Run(ctx); close(done) }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		c.collect()
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	<-done
}

func (c *Collector) collect() {
	lastError := ""
	for _, selected := range c.source.SelectedSources() {
		if code := c.collectSource(selected); code != "" {
			lastError = code
		}
	}
	c.mu.Lock()
	c.lastError = lastError
	c.mu.Unlock()
}

func (c *Collector) collectSource(selected sources.Selection) string {
	reader := c.readers[selected.ID]
	if reader == nil {
		history, err := sources.OpenHistory(selected)
		if err != nil {
			return "source-open"
		}
		reader = &collection{reader: history}
		c.readers[selected.ID] = reader
	}
	// Never advance the source cursor again until the previous batch is on disk.
	if err := c.box.Submit(reader.pending); err != nil {
		return "outbox-write"
	}
	reader.pending = nil
	states, err := reader.reader.Read()
	if err != nil {
		return "source-read"
	}
	for _, state := range states {
		if pending, ok := state.Object("autoAgents")["pending"].(int); ok && pending > 0 {
			c.mu.Lock()
			c.warning = "child-source-incomplete"
			c.mu.Unlock()
		}
		if problems, ok := state["agentSourceErrors"].([]string); ok && len(problems) > 0 {
			c.mu.Lock()
			c.warning = "child-source-incomplete"
			c.mu.Unlock()
		}
		revision := fmt.Sprint(state["turn"], ":", state["revision"])
		if revision == reader.lastRevision {
			continue
		}
		reader.pending = append(reader.pending, projection.FromGraph(state.String("provider"), state.String("sourceSessionId"), state)...)
		reader.lastRevision = revision
	}
	if err := c.box.Submit(reader.pending); err != nil {
		return "outbox-write"
	}
	reader.pending = nil
	return ""
}

func (c *Collector) Status() any {
	c.mu.Lock()
	lastError := c.lastError
	warning := c.warning
	c.mu.Unlock()
	return struct {
		Enabled           bool     `json:"enabled"`
		Destinations      []Status `json:"destinations"`
		CollectionError   string   `json:"collectionError,omitempty"`
		CollectionWarning string   `json:"collectionWarning,omitempty"`
	}{true, c.box.Status(), lastError, warning}
}
