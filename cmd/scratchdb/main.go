package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"scratchdb/internal/engine"
	sqlparser "scratchdb/internal/sql"
)

func main() {
	db, err := engine.Open("scratch.db.json")
	if err != nil {
		fmt.Fprintf(os.Stderr, "open database: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("ScratchDB")
	fmt.Println("Enter SQL statements terminated by ';'. Type '.exit' to quit.")

	reader := bufio.NewReader(os.Stdin)
	var buffer strings.Builder

	for {
		if buffer.Len() == 0 {
			fmt.Print("scratchdb> ")
		} else {
			fmt.Print("... ")
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}

		trimmed := strings.TrimSpace(line)
		if trimmed == ".exit" {
			fmt.Println("bye")
			return
		}
		if trimmed == "" {
			continue
		}

		buffer.WriteString(line)
		if !strings.Contains(line, ";") {
			continue
		}

		raw := buffer.String()
		buffer.Reset()

		statements := strings.Split(raw, ";")
		for _, chunk := range statements {
			sql := strings.TrimSpace(chunk)
			if sql == "" {
				continue
			}

			stmt, err := sqlparser.Parse(sql)
			if err != nil {
				fmt.Printf("parse error: %v\n", err)
				continue
			}

			result, err := db.Execute(stmt)
			if err != nil {
				fmt.Printf("execution error: %v\n", err)
				continue
			}

			printResult(result)
		}
	}
}

func printResult(result engine.Result) {
	if result.Message != "" {
		fmt.Println(result.Message)
	}
	if len(result.Columns) == 0 {
		return
	}

	fmt.Println(strings.Join(result.Columns, " | "))
	for _, row := range result.Rows {
		values := make([]string, 0, len(row))
		for _, value := range row {
			values = append(values, fmt.Sprintf("%v", value))
		}
		fmt.Println(strings.Join(values, " | "))
	}
	fmt.Printf("(%d rows)\n", len(result.Rows))
}
