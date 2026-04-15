package engine

import (
	"path/filepath"
	"testing"

	sqlparser "scratchdb/internal/sql"
)

func TestDatabaseLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db.json")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	_, err = db.Execute(sqlparser.CreateTableStatement{
		Name: "users",
		Columns: []sqlparser.ColumnDefinition{
			{Name: "id", Type: "INT", PrimaryKey: true},
			{Name: "name", Type: "TEXT"},
			{Name: "age", Type: "INT"},
		},
	})
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = db.Execute(sqlparser.InsertStatement{
		Table:   "users",
		Columns: []string{"id", "name", "age"},
		Values:  []any{int64(1), "Ada", int64(37)},
	})
	if err != nil {
		t.Fatalf("insert first row: %v", err)
	}

	_, err = db.Execute(sqlparser.InsertStatement{
		Table:   "users",
		Columns: []string{"id", "name", "age"},
		Values:  []any{int64(2), "Grace", int64(44)},
	})
	if err != nil {
		t.Fatalf("insert second row: %v", err)
	}

	result, err := db.Execute(sqlparser.SelectStatement{
		Table: "users",
		Where: &sqlparser.WhereClause{
			Column:   "id",
			Operator: ">=",
			Value:    int64(2),
		},
	})
	if err != nil {
		t.Fatalf("select rows: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 result row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "Grace" {
		t.Fatalf("expected Grace row, got %+v", result.Rows[0])
	}

	result, err = db.Execute(sqlparser.SelectStatement{
		Table:   "users",
		Columns: []string{"name"},
		Where: &sqlparser.WhereClause{
			Column:   "name",
			Operator: "=",
			Value:    "Ada",
		},
	})
	if err != nil {
		t.Fatalf("select projection by text: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != "Ada" {
		t.Fatalf("expected projected Ada row, got %+v", result.Rows)
	}

	result, err = db.Execute(sqlparser.SelectStatement{
		Table: "users",
		Where: &sqlparser.WhereClause{
			Column:   "id",
			Operator: ">",
			Value:    int64(1),
		},
	})
	if err != nil {
		t.Fatalf("select indexed greater-than: %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0] != int64(2) {
		t.Fatalf("expected id 2 row from greater-than query, got %+v", result.Rows)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}

	result, err = reopened.Execute(sqlparser.SelectStatement{Table: "users"})
	if err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows after reopen, got %d", len(result.Rows))
	}
}
