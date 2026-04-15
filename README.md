# ScratchDB

Build your own database in Go, from a B+ tree index to a tiny SQL engine.

ScratchDB is an educational database project designed to show how core database pieces fit together without hiding the important parts behind a huge codebase. It starts with a real B+ tree, layers on a small execution engine, and exposes everything through a lightweight SQL REPL.

This is not trying to compete with SQLite or Postgres. The goal is to make the internals understandable, hackable, and easy to extend.

## Why This Project Exists

Most database tutorials stop at theory, and most production databases are too large to use as a first code-reading experience. ScratchDB aims for the middle:

- Small enough to read in an afternoon
- Real enough to teach useful database ideas
- Structured enough to grow into a more serious engine
- Simple enough to modify without fear

If you want to learn how indexes, rows, parsing, execution, and persistence connect, this repo is meant to be a good starting point.

## Current Features

- `CREATE TABLE` with a single `INT PRIMARY KEY`
- `INSERT INTO ... VALUES (...)`
- `INSERT INTO ... (col, ...) VALUES (...)`
- `SELECT * FROM ...`
- `SELECT col1, col2 FROM ...`
- `WHERE` with `=`, `!=`, `>`, `>=`, `<`, `<=`
- B+ tree indexed lookups on the primary key
- Forward range scans using the B+ tree leaf chain
- Row filtering for non-indexed columns
- Durable JSON snapshot persistence between runs
- Interactive SQL REPL

## Example

```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);

INSERT INTO users (id, name, age) VALUES (1, 'Ada', 37);
INSERT INTO users VALUES (2, 'Grace', 44);
INSERT INTO users VALUES (3, 'Linus', 31);

SELECT * FROM users;
SELECT name, age FROM users WHERE id = 2;
SELECT * FROM users WHERE name = 'Ada';
SELECT * FROM users WHERE id >= 2;
```

## Project Layout

```text
.
├── cmd/scratchdb
│   └── main.go             # REPL entry point
└── internal
    ├── bptree
    │   ├── tree.go         # B+ tree insert, lookup, scan
    │   └── tree_test.go
    ├── engine
    │   ├── database.go     # Database lifecycle and statement execution
    │   ├── predicate.go    # WHERE clause evaluation
    │   ├── table.go        # Table storage, projection, filtering
    │   ├── types.go        # Shared engine types
    │   └── database_test.go
    └── sql
        ├── parser.go       # Tiny SQL parser
        └── parser_test.go
```

## Architecture

ScratchDB is split into a few focused layers:

### 1. B+ Tree

The B+ tree stores integer keys and row offsets.

- Leaf nodes hold keys and row pointers
- Internal nodes route lookups
- Leaf nodes are linked for fast range scans
- Primary-key queries use the tree directly

This keeps point lookups and ordered scans fast while staying compact enough to understand.

### 2. Table Engine

The table layer manages:

- Column definitions
- Row validation and normalization
- Primary-key enforcement
- Row projection for `SELECT name, age`
- Predicate filtering for `WHERE`

When possible, the engine uses the B+ tree. When no index applies, it falls back to scanning rows in index order.

### 3. SQL Layer

The SQL package parses a small subset of SQL into typed statements.

Right now the parser supports:

- `CREATE TABLE`
- `INSERT`
- `SELECT`
- Single-condition `WHERE` clauses

It is intentionally simple so the execution path stays easy to follow.

### 4. Persistence

Data is currently persisted as a JSON snapshot on disk.

That is not how a production database should store pages, but it makes the current version easy to inspect and debug. The code is structured so this can later be replaced with:

- Fixed-size pages
- Serialized B+ tree nodes
- Slotted row storage
- Write-ahead logging

## Getting Started

### Prerequisites

- Go 1.24 or newer

### Run the REPL

```bash
go run ./cmd/scratchdb
```

You will see:

```text
ScratchDB
Enter SQL statements terminated by ';'. Type '.exit' to quit.
```

### Run Tests

```bash
go test ./...
```

## Example Session

```text
scratchdb> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
created table users

scratchdb> INSERT INTO users VALUES (1, 'Ada', 37);
1 row inserted

scratchdb> INSERT INTO users VALUES (2, 'Grace', 44);
1 row inserted

scratchdb> SELECT * FROM users;
id | name | age
1 | Ada | 37
2 | Grace | 44
(2 rows)
```

## Supported SQL

### `CREATE TABLE`

```sql
CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
```

Rules:

- Exactly one primary key is required
- The primary key must be `INT`
- Supported column types are `INT` and `TEXT`

### `INSERT`

```sql
INSERT INTO users VALUES (1, 'Ada', 37);
INSERT INTO users (id, name, age) VALUES (2, 'Grace', 44);
```

### `SELECT`

```sql
SELECT * FROM users;
SELECT name, age FROM users;
SELECT * FROM users WHERE id = 1;
SELECT * FROM users WHERE id >= 2;
SELECT * FROM users WHERE name = 'Ada';
```

Current limitations:

- Only one table per query
- No joins
- No aggregate functions
- No `ORDER BY`
- No `DELETE` or `UPDATE` yet
- `WHERE` supports one condition only

## What Makes It Interesting

- The B+ tree is not mocked or faked; it is a real in-memory index
- Range scans work because leaf pages are linked
- Execution supports both indexed access and fallback scans
- The codebase is small enough for beginners to fully read
- The structure is clean enough to evolve into a real storage engine

## Roadmap

Planned improvements:

1. Replace JSON snapshots with page-based on-disk storage
2. Serialize B+ tree nodes directly into pages
3. Add `UPDATE` and `DELETE`
4. Support B+ tree deletion and node rebalancing
5. Add a lexer to simplify SQL parsing
6. Add transactions and a write-ahead log
7. Add multiple indexes per table
8. Add query planning for index selection

## Who This Is For

ScratchDB is a good fit if you are:

- Learning database internals
- Practicing systems programming in Go
- Building a portfolio project for GitHub
- Looking for a small codebase to extend in public
- Curious how SQL maps onto data structures

## Contributing

Contributions are welcome, especially if they keep the project educational and approachable.

Good contribution areas:

- Storage engine improvements
- Better parser structure
- More SQL statements
- More tests
- Better error messages
- Documentation and diagrams

## Status

ScratchDB is an educational work in progress. The architecture is deliberate, but the engine is still early-stage and intentionally limited.

If you want to follow the journey from toy database to something more serious, this repo is set up for exactly that.

## License

Add a license file before publishing if you want others to reuse the code more easily.
