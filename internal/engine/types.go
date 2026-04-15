package engine

import "fmt"

type ColumnType string

const (
	IntType  ColumnType = "INT"
	TextType ColumnType = "TEXT"
)

type Column struct {
	Name       string     `json:"name"`
	Type       ColumnType `json:"type"`
	PrimaryKey bool       `json:"primary_key"`
}

type Row map[string]any

type Result struct {
	Message string
	Columns []string
	Rows    [][]any
}

func normalizeValue(raw any, columnType ColumnType) (any, error) {
	switch columnType {
	case IntType:
		switch v := raw.(type) {
		case int:
			return int64(v), nil
		case int64:
			return v, nil
		case float64:
			return int64(v), nil
		default:
			return nil, fmt.Errorf("expected INT value, got %T", raw)
		}
	case TextType:
		text, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("expected TEXT value, got %T", raw)
		}
		return text, nil
	default:
		return nil, fmt.Errorf("unsupported column type %s", columnType)
	}
}
