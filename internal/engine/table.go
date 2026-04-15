package engine

import (
	"fmt"

	"scratchdb/internal/bptree"
)

type table struct {
	Name           string     `json:"name"`
	Columns        []Column   `json:"columns"`
	Rows           []Row      `json:"rows"`
	PrimaryKey     string     `json:"primary_key"`
	PrimaryKeyType ColumnType `json:"primary_key_type"`

	index *bptree.Tree
}

func newTable(name string, columns []Column) (*table, error) {
	var pk *Column
	for i := range columns {
		if columns[i].PrimaryKey {
			if pk != nil {
				return nil, fmt.Errorf("only one primary key is supported")
			}
			pk = &columns[i]
		}
	}
	if pk == nil {
		return nil, fmt.Errorf("table %s must define a primary key", name)
	}
	if pk.Type != IntType {
		return nil, fmt.Errorf("primary key must be INT")
	}

	return &table{
		Name:           name,
		Columns:        columns,
		PrimaryKey:     pk.Name,
		PrimaryKeyType: pk.Type,
		index:          bptree.New(4),
	}, nil
}

func (t *table) rebuildIndex() error {
	t.index = bptree.New(4)
	for i := range t.Rows {
		row, err := t.normalizeRow(t.Rows[i])
		if err != nil {
			return err
		}
		t.Rows[i] = row

		key, err := t.primaryKeyValue(row)
		if err != nil {
			return err
		}
		if err := t.index.Insert(key, uint64(i)); err != nil {
			return err
		}
	}
	return nil
}

func (t *table) insertWithColumns(columns []string, values []any) error {
	row, err := t.rowFromInput(columns, values)
	if err != nil {
		return err
	}

	key, err := t.primaryKeyValue(row)
	if err != nil {
		return err
	}
	if _, exists := t.index.Get(key); exists {
		return fmt.Errorf("duplicate primary key %d", key)
	}

	offset := uint64(len(t.Rows))
	if err := t.index.Insert(key, offset); err != nil {
		return err
	}
	t.Rows = append(t.Rows, row)
	return nil
}

func (t *table) rowFromInput(columns []string, values []any) (Row, error) {
	if len(columns) == 0 {
		if len(values) != len(t.Columns) {
			return nil, fmt.Errorf("expected %d values, got %d", len(t.Columns), len(values))
		}

		row := make(Row, len(t.Columns))
		for i, column := range t.Columns {
			value, err := normalizeValue(values[i], column.Type)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", column.Name, err)
			}
			row[column.Name] = value
		}
		return row, nil
	}

	if len(columns) != len(values) {
		return nil, fmt.Errorf("expected %d values for provided column list, got %d", len(columns), len(values))
	}

	row := make(Row, len(t.Columns))
	seen := make(map[string]struct{}, len(columns))
	for i, name := range columns {
		column, ok := t.lookupColumn(name)
		if !ok {
			return nil, fmt.Errorf("unknown column %s", name)
		}
		if _, exists := seen[column.Name]; exists {
			return nil, fmt.Errorf("duplicate column %s", column.Name)
		}
		seen[column.Name] = struct{}{}

		value, err := normalizeValue(values[i], column.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", column.Name, err)
		}
		row[column.Name] = value
	}

	for _, column := range t.Columns {
		if _, ok := row[column.Name]; !ok {
			return nil, fmt.Errorf("missing value for column %s", column.Name)
		}
	}
	return row, nil
}

func (t *table) selectRows(columns []string, where *predicate) ([][]any, error) {
	sourceRows, err := t.filteredRows(where)
	if err != nil {
		return nil, err
	}

	projection, err := t.resolveProjection(columns)
	if err != nil {
		return nil, err
	}

	rows := make([][]any, 0, len(sourceRows))
	for _, row := range sourceRows {
		rows = append(rows, t.projectRowWithColumns(row, projection))
	}
	return rows, nil
}

func (t *table) projectedColumnNames(columns []string) ([]string, error) {
	projection, err := t.resolveProjection(columns)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(projection))
	for _, column := range projection {
		names = append(names, column.Name)
	}
	return names, nil
}

func (t *table) filteredRows(where *predicate) ([]Row, error) {
	if where == nil {
		return t.rowsInIndexOrder(), nil
	}

	if matches, ok, err := t.tryIndexedLookup(where); ok || err != nil {
		return matches, err
	}

	rows := t.rowsInIndexOrder()
	filtered := make([]Row, 0, len(rows))
	for _, row := range rows {
		match, err := where.matches(row)
		if err != nil {
			return nil, err
		}
		if match {
			filtered = append(filtered, row)
		}
	}
	return filtered, nil
}

func (t *table) tryIndexedLookup(where *predicate) ([]Row, bool, error) {
	if where.Column != t.PrimaryKey {
		return nil, false, nil
	}

	value, ok := where.Value.(int64)
	if !ok {
		return nil, false, fmt.Errorf("primary key filters must use INT literals")
	}

	switch where.Operator {
	case "=":
		offset, found := t.index.Get(value)
		if !found {
			return nil, true, nil
		}
		return []Row{t.Rows[offset]}, true, nil
	case ">=":
		return t.rowsForIndexScan(t.index.ScanFrom(value)), true, nil
	case ">":
		kvs := t.index.ScanFrom(value)
		rows := make([]Row, 0, len(kvs))
		for _, kv := range kvs {
			row := t.Rows[kv.Value]
			key, err := t.primaryKeyValue(row)
			if err != nil {
				return nil, true, err
			}
			if key > value {
				rows = append(rows, row)
			}
		}
		return rows, true, nil
	default:
		return nil, false, nil
	}
}

func (t *table) rowsForIndexScan(kvs []bptree.KV) []Row {
	rows := make([]Row, 0, len(kvs))
	for _, kv := range kvs {
		rows = append(rows, t.Rows[kv.Value])
	}
	return rows
}

func (t *table) rowsInIndexOrder() []Row {
	return t.rowsForIndexScan(t.index.All())
}

func (t *table) resolveProjection(columns []string) ([]Column, error) {
	if len(columns) == 0 {
		return append([]Column(nil), t.Columns...), nil
	}

	projection := make([]Column, 0, len(columns))
	for _, name := range columns {
		column, ok := t.lookupColumn(name)
		if !ok {
			return nil, fmt.Errorf("unknown column %s", name)
		}
		projection = append(projection, column)
	}
	return projection, nil
}

func (t *table) projectRowWithColumns(row Row, columns []Column) []any {
	values := make([]any, 0, len(columns))
	for _, column := range columns {
		values = append(values, row[column.Name])
	}
	return values
}

func (t *table) lookupColumn(name string) (Column, bool) {
	for _, column := range t.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return Column{}, false
}

func (t *table) primaryKeyValue(row Row) (int64, error) {
	value, ok := row[t.PrimaryKey]
	if !ok {
		return 0, fmt.Errorf("missing primary key column %s", t.PrimaryKey)
	}
	key, ok := value.(int64)
	if !ok {
		return 0, fmt.Errorf("primary key column %s must be INT", t.PrimaryKey)
	}
	return key, nil
}

func (t *table) normalizeRow(row Row) (Row, error) {
	normalized := make(Row, len(t.Columns))
	for _, column := range t.Columns {
		value, ok := row[column.Name]
		if !ok {
			return nil, fmt.Errorf("missing column %s", column.Name)
		}

		cleanValue, err := normalizeValue(value, column.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", column.Name, err)
		}
		normalized[column.Name] = cleanValue
	}
	return normalized, nil
}
