// Package bootstrap composes selected source and delivery implementations.
package bootstrap

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	exportadapter "agent-observer/internal/adapters/export"
	"agent-observer/internal/delivery"
)

type ExportConfig struct {
	Outbox       string              `json:"outbox"`
	Destinations []DestinationConfig `json:"destinations"`
}
type DestinationConfig struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	Endpoint       string `json:"endpoint"`
	Project        string `json:"project,omitempty"`
	PublicKeyEnv   string `json:"publicKeyEnv,omitempty"`
	SecretKeyEnv   string `json:"secretKeyEnv,omitempty"`
	APIKeyEnv      string `json:"apiKeyEnv,omitempty"`
	WorkspaceIDEnv string `json:"workspaceIdEnv,omitempty"`
	HeadersEnv     string `json:"headersEnv,omitempty"`
}

// LoadExports reads explicit configuration. Secret values stay in the environment
// and are never written to outbox records or diagnostics.
func LoadExports(path string) (*delivery.Outbox, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("cannot open export configuration")
	}
	defer f.Close()
	decoder := json.NewDecoder(io.LimitReader(f, 1024*1024))
	decoder.DisallowUnknownFields()
	var config ExportConfig
	if decoder.Decode(&config) != nil {
		return nil, errors.New("invalid export configuration")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("trailing export configuration data")
	}
	if !filepath.IsAbs(config.Outbox) || len(config.Destinations) == 0 {
		return nil, errors.New("absolute outbox and destinations required")
	}
	destinations := make([]delivery.Destination, 0, len(config.Destinations))
	for _, d := range config.Destinations {
		if strings.TrimSpace(d.ID) == "" || len(d.ID) > 200 {
			return nil, errors.New("explicit logical destination id required")
		}
		exporter, err := makeExporter(d)
		if err != nil {
			return nil, err
		}
		scope, _ := json.Marshal([]string{d.ID, d.Project})
		destinations = append(destinations, delivery.Destination{ID: delivery.FingerprintDestination(d.Type, d.Endpoint, string(scope)), Exporter: exporter})
	}
	return delivery.New(config.Outbox, destinations)
}
func requiredEnv(name string) (string, error) {
	if name == "" {
		return "", errors.New("credential environment variable name required")
	}
	value := os.Getenv(name)
	if value == "" {
		return "", errors.New("required credential environment variable is empty")
	}
	return value, nil
}
func makeExporter(d DestinationConfig) (delivery.Exporter, error) {
	switch d.Type {
	case "otlp":
		headers := map[string]string{}
		if d.HeadersEnv != "" {
			raw, err := requiredEnv(d.HeadersEnv)
			if err != nil {
				return nil, err
			}
			if json.Unmarshal([]byte(raw), &headers) != nil {
				return nil, errors.New("invalid OTLP headers environment value")
			}
		}
		return exportadapter.NewOTLP(exportadapter.OTLPConfig{Endpoint: d.Endpoint, Headers: headers})
	case "langfuse":
		public, err := requiredEnv(d.PublicKeyEnv)
		if err != nil {
			return nil, err
		}
		secret, err := requiredEnv(d.SecretKeyEnv)
		if err != nil {
			return nil, err
		}
		return exportadapter.NewLangfuse(exportadapter.LangfuseConfig{Endpoint: d.Endpoint, PublicKey: public, SecretKey: secret})
	case "langsmith":
		key, err := requiredEnv(d.APIKeyEnv)
		if err != nil {
			return nil, err
		}
		workspace := ""
		if d.WorkspaceIDEnv != "" {
			workspace, err = requiredEnv(d.WorkspaceIDEnv)
			if err != nil {
				return nil, err
			}
		}
		return exportadapter.NewLangSmith(exportadapter.LangSmithConfig{Endpoint: d.Endpoint, APIKey: key, WorkspaceID: workspace, Project: d.Project})
	default:
		return nil, errors.New("unknown export destination type")
	}
}
