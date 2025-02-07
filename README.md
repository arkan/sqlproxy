# Overview

This project provides a custom Go database/sql driver that communicates with an external Go service (proxy) instead of directly interfacing with a database. The goal is to work around the limitations of an ODBC driver that only functions on 32-bit Windows, allowing development on macOS.

# Motivation

I encountered an issue where my required ODBC driver only supports 32-bit Windows, making it unusable for development on my Mac. To solve this, I created a lightweight SQL driver that forwards queries to an intermediary service (proxy), which connects to a real database.

# How It Works

- sqlproxy: Implements Go's database/sql/driver interface, sending queries to queryservice via TCP.
- proxy: A Go service that executes SQL queries on an actual database (e.g., SQLite, PostgreSQL, MySQL) and returns results.
- Seamless Integration: This driver allows existing Go applications to use database/sql as if they were connecting to a regular database.

# Usage
Run queryservice

```
go run queryservice/main.go
```

Use customdriver in Your Go Code

```
go run queryservice/main.go
```
Use customdriver in Your Go Code

```
import (
    "database/sql"
    _ "github.com/arkan/sqlproxy/driver"
)

func main() {
    db, err := sql.Open("sqlproxy", "localhost:8888")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    rows, err := db.Query("SELECT id, name FROM users")
    if err != nil {
        panic(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int
        var name string
        if err := rows.Scan(&id, &name); err != nil {
            panic(err)
        }
        fmt.Println("User:", id, name)
    }
}
```

# License
This project is licensed under the MIT License.

