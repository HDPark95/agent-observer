// Package model defines JSON records shared by modules, including explicitly collected message bodies.
package model

type Record map[string]any

func (r Record) String(k string) string { s, _ := r[k].(string); return s }
func (r Record) Object(k string) Record {
	switch v := r[k].(type) {
	case map[string]any:
		return Record(v)
	case Record:
		return v
	}
	return Record{}
}
func (r Record) Array(k string) []any    { v, _ := r[k].([]any); return v }
func (r Record) Bool(k string) bool      { v, _ := r[k].(bool); return v }
func (r Record) Number(k string) float64 { v, _ := r[k].(float64); return v }
func (r Record) Clone() Record {
	c := Record{}
	for k, v := range r {
		c[k] = v
	}
	return c
}
func As(v any) Record {
	switch r := v.(type) {
	case map[string]any:
		return Record(r)
	case Record:
		return r
	}
	return Record{}
}
func Nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}
