package sql

import "testing"

func TestParseCreateTable(t *testing.T) {
	stmt, err := Parse("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)")
	if err != nil {
		t.Fatalf("parse create table: %v", err)
	}

	createStmt, ok := stmt.(CreateTableStatement)
	if !ok {
		t.Fatalf("expected CreateTableStatement, got %T", stmt)
	}

	if createStmt.Name != "users" || len(createStmt.Columns) != 3 {
		t.Fatalf("unexpected statement: %+v", createStmt)
	}
}

func TestParseInsertAndSelect(t *testing.T) {
	stmt, err := Parse("INSERT INTO users (id, name, age) VALUES (1, 'Ada', 37)")
	if err != nil {
		t.Fatalf("parse insert: %v", err)
	}
	insertStmt := stmt.(InsertStatement)
	if insertStmt.Table != "users" || len(insertStmt.Values) != 3 || len(insertStmt.Columns) != 3 {
		t.Fatalf("unexpected insert statement: %+v", insertStmt)
	}

	stmt, err = Parse("SELECT name, age FROM users WHERE id >= 2")
	if err != nil {
		t.Fatalf("parse select: %v", err)
	}
	selectStmt := stmt.(SelectStatement)
	if selectStmt.Where == nil || selectStmt.Where.Operator != ">=" || len(selectStmt.Columns) != 2 {
		t.Fatalf("unexpected select statement: %+v", selectStmt)
	}
}
