package sql

import (
	"fmt"
	"strconv"
	"strings"
)

type Statement interface {
	isStatement()
}

type CreateTableStatement struct {
	Name    string
	Columns []ColumnDefinition
}

type InsertStatement struct {
	Table   string
	Columns []string
	Values  []any
}

type SelectStatement struct {
	Table   string
	Columns []string
	Where   *WhereClause
}

type ColumnDefinition struct {
	Name       string
	Type       string
	PrimaryKey bool
}

type WhereClause struct {
	Column   string
	Operator string
	Value    any
}

func (CreateTableStatement) isStatement() {}
func (InsertStatement) isStatement()      {}
func (SelectStatement) isStatement()      {}

func Parse(input string) (Statement, error) {
	input = strings.TrimSpace(input)
	upper := strings.ToUpper(input)

	switch {
	case strings.HasPrefix(upper, "CREATE TABLE "):
		return parseCreateTable(input)
	case strings.HasPrefix(upper, "INSERT INTO "):
		return parseInsert(input)
	case strings.HasPrefix(upper, "SELECT "):
		return parseSelect(input)
	default:
		return nil, fmt.Errorf("unsupported statement")
	}
}

func parseCreateTable(input string) (Statement, error) {
	openParen := strings.Index(input, "(")
	closeParen := strings.LastIndex(input, ")")
	if openParen == -1 || closeParen == -1 || closeParen <= openParen {
		return nil, fmt.Errorf("CREATE TABLE must define column list")
	}

	header := strings.TrimSpace(input[:openParen])
	parts := strings.Fields(header)
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}

	rawColumns := splitCSV(input[openParen+1 : closeParen])
	columns := make([]ColumnDefinition, 0, len(rawColumns))
	for _, raw := range rawColumns {
		column, err := parseColumnDefinition(raw)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	return CreateTableStatement{
		Name:    parts[2],
		Columns: columns,
	}, nil
}

func parseColumnDefinition(input string) (ColumnDefinition, error) {
	parts := strings.Fields(strings.TrimSpace(input))
	if len(parts) < 2 {
		return ColumnDefinition{}, fmt.Errorf("invalid column definition %q", input)
	}

	column := ColumnDefinition{
		Name: parts[0],
		Type: strings.ToUpper(parts[1]),
	}

	if len(parts) > 2 {
		if len(parts) != 4 || strings.ToUpper(parts[2]) != "PRIMARY" || strings.ToUpper(parts[3]) != "KEY" {
			return ColumnDefinition{}, fmt.Errorf("unsupported column modifier in %q", input)
		}
		column.PrimaryKey = true
	}

	return column, nil
}

func parseInsert(input string) (Statement, error) {
	upper := strings.ToUpper(input)
	valuesIndex := strings.Index(upper, " VALUES ")
	if valuesIndex == -1 {
		return nil, fmt.Errorf("INSERT must include VALUES")
	}

	head := strings.TrimSpace(input[:valuesIndex])
	if !strings.HasPrefix(strings.ToUpper(head), "INSERT INTO ") {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}

	target := strings.TrimSpace(head[len("INSERT INTO "):])
	table := target
	var columns []string

	if openParen := strings.Index(target, "("); openParen != -1 {
		closeParen := strings.LastIndex(target, ")")
		if closeParen == -1 || closeParen < openParen {
			return nil, fmt.Errorf("invalid INSERT column list")
		}
		table = strings.TrimSpace(target[:openParen])
		rawColumns := splitCSV(target[openParen+1 : closeParen])
		columns = make([]string, 0, len(rawColumns))
		for _, column := range rawColumns {
			columns = append(columns, strings.ToLower(strings.TrimSpace(column)))
		}
	}

	valuesPart := strings.TrimSpace(input[valuesIndex+len(" VALUES "):])
	if !strings.HasPrefix(valuesPart, "(") || !strings.HasSuffix(valuesPart, ")") {
		return nil, fmt.Errorf("VALUES must be parenthesized")
	}

	rawValues := splitCSV(valuesPart[1 : len(valuesPart)-1])
	values := make([]any, 0, len(rawValues))
	for _, raw := range rawValues {
		value, err := parseLiteral(raw)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	return InsertStatement{
		Table:   strings.TrimSpace(table),
		Columns: columns,
		Values:  values,
	}, nil
}

func parseSelect(input string) (Statement, error) {
	upper := strings.ToUpper(input)
	if !strings.HasPrefix(upper, "SELECT ") {
		return nil, fmt.Errorf("invalid SELECT syntax")
	}

	fromIndex := strings.Index(upper, " FROM ")
	if fromIndex == -1 {
		return nil, fmt.Errorf("SELECT must include FROM")
	}

	columnPart := strings.TrimSpace(input[len("SELECT "):fromIndex])
	columns := parseSelectColumns(columnPart)

	rest := strings.TrimSpace(input[fromIndex+len(" FROM "):])
	whereIndex := strings.Index(strings.ToUpper(rest), " WHERE ")
	if whereIndex == -1 {
		return SelectStatement{
			Table:   strings.TrimSpace(rest),
			Columns: columns,
		}, nil
	}

	table := strings.TrimSpace(rest[:whereIndex])
	where, err := parseWhere(rest[whereIndex+len(" WHERE "):])
	if err != nil {
		return nil, err
	}

	return SelectStatement{
		Table:   table,
		Columns: columns,
		Where:   where,
	}, nil
}

func parseWhere(input string) (*WhereClause, error) {
	for _, operator := range []string{">=", "<=", "!=", ">", "<", "="} {
		if idx := strings.Index(input, operator); idx != -1 {
			column := strings.TrimSpace(input[:idx])
			rawValue := strings.TrimSpace(input[idx+len(operator):])
			value, err := parseLiteral(rawValue)
			if err != nil {
				return nil, err
			}
			return &WhereClause{
				Column:   strings.ToLower(column),
				Operator: operator,
				Value:    value,
			}, nil
		}
	}
	return nil, fmt.Errorf("unsupported WHERE clause")
}

func parseSelectColumns(input string) []string {
	if strings.TrimSpace(input) == "*" {
		return nil
	}

	rawColumns := splitCSV(input)
	columns := make([]string, 0, len(rawColumns))
	for _, column := range rawColumns {
		columns = append(columns, strings.ToLower(strings.TrimSpace(column)))
	}
	return columns
}

func parseLiteral(input string) (any, error) {
	input = strings.TrimSpace(input)
	if strings.HasPrefix(input, "'") && strings.HasSuffix(input, "'") && len(input) >= 2 {
		return strings.ReplaceAll(input[1:len(input)-1], "''", "'"), nil
	}

	number, err := strconv.ParseInt(input, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unsupported literal %q", input)
	}
	return number, nil
}

func splitCSV(input string) []string {
	var parts []string
	var current strings.Builder
	inString := false

	for i := 0; i < len(input); i++ {
		ch := input[i]
		switch ch {
		case '\'':
			if inString && i+1 < len(input) && input[i+1] == '\'' {
				current.WriteByte(ch)
				current.WriteByte(input[i+1])
				i++
				continue
			}
			inString = !inString
			current.WriteByte(ch)
		case ',':
			if inString {
				current.WriteByte(ch)
				continue
			}
			parts = append(parts, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, strings.TrimSpace(current.String()))
	}
	return parts
}
