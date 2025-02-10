package main

import (
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	_ "github.com/alexbrainman/odbc"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

const listenAddr = ":8888"

// Query request struct.
type QueryRequest struct {
	Query string        `msgpack:"query"`
	Args  []interface{} `msgpack:"args"`
}

// Query response struct.
type QueryResponse struct {
	Columns []string        `msgpack:"columns"`
	Data    [][]interface{} `msgpack:"data"`
}

// Exec request struct.
type ExecRequest struct {
	Query string        `msgpack:"query"`
	Args  []interface{} `msgpack:"args"`
}

// Exec response struct.
type ExecResponse struct {
	RowsAffected int64 `msgpack:"rows_affected"`
	LastInsertID int64 `msgpack:"last_insert_id"`
}

var (
	dsn = flag.String("dsn", "", "DSN to connect to")
)

func main() {
	flag.Parse()
	if *dsn == "" {
		log.Fatal("DSN is required")
	}

	db, err := sql.Open("odbc", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(errors.Wrap(err, "failed to ping database"))
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on %s...\n", listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Connection error:", err)
			continue
		}

		go handleConnection(conn, db)
	}
}

func handleConnection(conn net.Conn, db *sql.DB) {
	defer conn.Close()

	for {
		var lengthBytes [4]byte
		_, err := io.ReadFull(conn, lengthBytes[:])
		if err != nil {
			log.Println("Read length error:", err)
			return
		}
		length := binary.BigEndian.Uint32(lengthBytes[:])

		requestData := make([]byte, length)
		if _, err := io.ReadFull(conn, requestData); err != nil {
			return
		}

		if isQuery(requestData) {
			if err := handleQuery(conn, db, requestData); err != nil {
				return
			}
		} else {
			if err := handleExec(conn, db, requestData); err != nil {
				return
			}
		}
	}
}

func isQuery(data []byte) bool {
	var temp QueryRequest
	if err := msgpack.Unmarshal(data, &temp); err != nil {
		return false
	}
	// Remove whitespace from the query.
	query := strings.TrimSpace(temp.Query)

	// Extract the first word of the query (ignoring case).
	firstWord := strings.ToUpper(strings.Fields(query)[0])

	// Check if it starts with SELECT (indicating a query).
	return firstWord == "SELECT"
}

func handleQuery(conn net.Conn, db *sql.DB, data []byte) error {
	var req QueryRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return err
	}

	fmt.Printf("handleQuery: %s - %v\n", req.Query, req.Args)

	rows, err := db.Query(req.Query, req.Args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	var results [][]interface{}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		pointers := make([]interface{}, len(cols))
		for i := range values {
			pointers[i] = &values[i]
		}
		rows.Scan(pointers...)
		results = append(results, values)
	}

	sendResponse(conn, QueryResponse{Columns: cols, Data: results})

	return nil
}

func handleExec(conn net.Conn, db *sql.DB, data []byte) error {
	var req ExecRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return err
	}

	fmt.Printf("handleExec: %s - %v\n", req.Query, req.Args)

	result, err := db.Exec(req.Query, req.Args...)
	if err != nil {
		return err
	}

	// Get the number of rows affected and the last inserted ID.
	// We don't care about the errors, because some databases don't support it.
	rows, _ := result.RowsAffected()
	lastID, _ := result.LastInsertId()

	sendResponse(conn, ExecResponse{RowsAffected: rows, LastInsertID: lastID})

	return nil
}

func sendResponse(conn net.Conn, response interface{}) {
	data, err := msgpack.Marshal(response)
	if err != nil {
		return
	}

	// Convert length to fixed 4-byte array (BigEndian)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(data)))

	// Write response length
	_, err = conn.Write(length[:])
	if err != nil {
		log.Println("Write length error:", err)
		return
	}

	// Write actual response
	_, err = conn.Write(data)
	if err != nil {
		log.Println("Write response error:", err)
	}
}
