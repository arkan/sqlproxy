package main

import (
	"database/sql"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"

	_ "github.com/alexbrainman/odbc"
	"github.com/pkg/errors"
	"github.com/vmihailenco/msgpack"
)

const listenAddr = ":8888"

type QueryRequest struct {
	Query string        `msgpack:"query"`
	Args  []interface{} `msgpack:"args"`
}

type QueryResponse struct {
	Columns []string        `msgpack:"columns"`
	Data    [][]interface{} `msgpack:"data"`
}

type ExecRequest struct {
	Query string        `msgpack:"query"`
	Args  []interface{} `msgpack:"args"`
}

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
			handleQuery(conn, db, requestData)
		} else {
			handleExec(conn, db, requestData)
		}
	}
}

func isQuery(data []byte) bool {
	var temp QueryRequest
	return msgpack.Unmarshal(data, &temp) == nil
}

func handleQuery(conn net.Conn, db *sql.DB, data []byte) {
	var req QueryRequest
	msgpack.Unmarshal(data, &req)

	log.Printf("handleQuery: %s - %v", req.Query, req.Args)

	rows, _ := db.Query(req.Query, req.Args...)
	defer rows.Close()

	cols, _ := rows.Columns()
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
}

func handleExec(conn net.Conn, db *sql.DB, data []byte) {
	var req ExecRequest
	msgpack.Unmarshal(data, &req)

	log.Printf("handleExec: %s - %v", req.Query, req.Args)

	result, _ := db.Exec(req.Query, req.Args...)
	rows, _ := result.RowsAffected()
	lastID, _ := result.LastInsertId()

	sendResponse(conn, ExecResponse{RowsAffected: rows, LastInsertID: lastID})
}

func sendResponse(conn net.Conn, response interface{}) {
	data, err := msgpack.Marshal(response)
	if err != nil {
		log.Println("msgpack marshal error:", err)
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
