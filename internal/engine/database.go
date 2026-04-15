package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	sqlparser "scratchdb/internal/sql"
)

type Database struct {
	path   string
	Tables map[string]*table `json:"tables"`
}

func Open(path string) (*Database, error) {
	db := &Database{
		path:   path,
		Tables: make(map[string]*table),
	}

	bytes, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return db, nil
		}
		return nil, err
	}

	if err := json.Unmarshal(bytes, db); err != nil {
		return nil, err
	}
	db.path = path
	for _, table := range db.Tables {
		if err := table.rebuildIndex(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (db *Database) Execute(statement sqlparser.Statement) (Result, error) {
	switch stmt := statement.(type) {
	case sqlparser.CreateTableStatement:
		return db.createTable(stmt)
	case sqlparser.InsertStatement:
		return db.insert(stmt)
	case sqlparser.SelectStatement:
		return db.selectRows(stmt)
	default:
		return Result{}, fmt.Errorf("unsupported statement %T", statement)
	}
}

func (db *Database) createTable(stmt sqlparser.CreateTableStatement) (Result, error) {
	name := strings.ToLower(stmt.Name)
	if _, exists := db.Tables[name]; exists {
		return Result{}, fmt.Errorf("table %s already exists", stmt.Name)
	}

	columns := make([]Column, 0, len(stmt.Columns))
	for _, column := range stmt.Columns {
		columns = append(columns, Column{
			Name:       strings.ToLower(column.Name),
			Type:       ColumnType(strings.ToUpper(column.Type)),
			PrimaryKey: column.PrimaryKey,
		})
	}

	table, err := newTable(name, columns)
	if err != nil {
		return Result{}, err
	}
	db.Tables[name] = table

	if err := db.persist(); err != nil {
		return Result{}, err
	}
	return Result{Message: fmt.Sprintf("created table %s", stmt.Name)}, nil
}

func (db *Database) insert(stmt sqlparser.InsertStatement) (Result, error) {
	table, err := db.table(stmt.Table)
	if err != nil {
		return Result{}, err
	}
	if err := table.insertWithColumns(stmt.Columns, stmt.Values); err != nil {
		return Result{}, err
	}

	if err := db.persist(); err != nil {
		return Result{}, err
	}
	return Result{Message: "1 row inserted"}, nil
}

func (db *Database) selectRows(stmt sqlparser.SelectStatement) (Result, error) {
	table, err := db.table(stmt.Table)
	if err != nil {
		return Result{}, err
	}

	var pred *predicate
	if stmt.Where != nil {
		pred = &predicate{
			Column:   strings.ToLower(stmt.Where.Column),
			Operator: stmt.Where.Operator,
			Value:    stmt.Where.Value,
		}
	}

	rows, err := table.selectRows(stmt.Columns, pred)
	if err != nil {
		return Result{}, err
	}
	columnNames, err := table.projectedColumnNames(stmt.Columns)
	if err != nil {
		return Result{}, err
	}

	return Result{
		Columns: columnNames,
		Rows:    rows,
	}, nil
}

func (db *Database) table(name string) (*table, error) {
	table, ok := db.Tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", name)
	}
	return table, nil
}

func (db *Database) persist() error {
	bytes, err := json.MarshalIndent(db, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(db.path, bytes, 0o644)
}
