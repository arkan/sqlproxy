package driver

import (
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/vmihailenco/msgpack"
)

// Register driver.
func init() {
	sql.Register("sqlproxy", &Driver{})
}

// Driver implementation.
type Driver struct{}

// Open a connection to the proxy.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	conn, err := net.Dial("tcp", dsn)
	if err != nil {
		return nil, err
	}

	return &Conn{conn: conn}, nil
}

// Connection implementation.
type Conn struct {
	conn net.Conn
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return &Stmt{conn: c.conn, query: query}, nil
}

// Close the connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions are not supported")
}

// Statement implementation
type Stmt struct {
	conn  net.Conn
	query string
}

// Close the statement.
func (s *Stmt) Close() error {
	return nil // No-op
}

// Number of input parameters.
func (s *Stmt) NumInput() int {
	return -1 // Variable number of parameters
}

// Query request/response structs
type QueryRequest struct {
	Query string         `msgpack:"query"`
	Args  []driver.Value `msgpack:"args"`
}

// Query response struct.
type QueryResponse struct {
	Columns []string         `msgpack:"columns"`
	Data    [][]driver.Value `msgpack:"data"`
}

// Exec request/response structs
type ExecRequest struct {
	Query string         `msgpack:"query"`
	Args  []driver.Value `msgpack:"args"`
}

// Exec response struct.
type ExecResponse struct {
	RowsAffected int64 `msgpack:"rows_affected"`
	LastInsertID int64 `msgpack:"last_insert_id"`
}

// Query execution.
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	request := QueryRequest{Query: s.query, Args: args}
	err := sendRequest(s.conn, request)
	if err != nil {
		return nil, err
	}

	response, err := readQueryResponse(s.conn)
	if err != nil {
		return nil, err
	}

	return &Rows{columns: response.Columns, data: response.Data}, nil
}

// Exec execution.
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	request := ExecRequest{Query: s.query, Args: args}
	err := sendRequest(s.conn, request)
	if err != nil {
		return nil, err
	}

	response, err := readExecResponse(s.conn)
	if err != nil {
		return nil, err
	}

	return &Result{lastInsertID: response.LastInsertID, rowsAffected: response.RowsAffected}, nil
}

// Rows implementation
type Rows struct {
	columns []string
	data    [][]driver.Value
	index   int
}

// Columns.
func (r *Rows) Columns() []string {
	return r.columns
}

// Next row.
func (r *Rows) Next(dest []driver.Value) error {
	if r.index >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.index])
	r.index++
	return nil
}

// Close the rows.
func (r *Rows) Close() error {
	return nil
}

// Result implementation.
type Result struct {
	lastInsertID int64
	rowsAffected int64
}

func (r *Result) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r *Result) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// Helper functions.
func sendRequest(conn net.Conn, request interface{}) error {
	data, err := msgpack.Marshal(request)
	if err != nil {
		return err
	}

	length := uint32(len(data))
	err = binary.Write(conn, binary.BigEndian, length)
	if err != nil {
		return err
	}

	_, err = conn.Write(data)
	return err
}

func readQueryResponse(conn net.Conn) (*QueryResponse, error) {
	// Read fixed 4-byte length prefix.
	var lengthBytes [4]byte
	_, err := io.ReadFull(conn, lengthBytes[:])
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBytes[:])

	// Read the actual data.
	data := make([]byte, length)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, err
	}

	// Decode msgpack.
	var response QueryResponse
	err = msgpack.Unmarshal(data, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}

func readExecResponse(conn net.Conn) (*ExecResponse, error) {
	var lengthBytes [4]byte
	_, err := io.ReadFull(conn, lengthBytes[:])
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBytes[:])

	data := make([]byte, length)
	_, err = io.ReadFull(conn, data)
	if err != nil {
		return nil, err
	}

	var response ExecResponse
	err = msgpack.Unmarshal(data, &response)
	if err != nil {
		return nil, err
	}

	return &response, nil
}
