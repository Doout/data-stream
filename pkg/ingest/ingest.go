package ingest

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/doout/data-stream/pkg/types"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Config struct {

	//The minimum number of connections to maintain open
	MinConnection int

	//TODO add AutoScaling to ingest
	//The maximum number of connections to open, default 10.
	//MaxConnections int

	//The number of rows to commit at a time
	CommitSize int

	//Table name to insert the data into
	TableName string

	//TODO: Add the column names to insert the data into
	//What are the Columns names in your table, if left empty, set NumberOfColumns
	//Columns []string

	//If Columns is not set, create the sql prepare statement with x of columns using NumberOfColumns
	NumberOfColumns int
}

type Ingest struct {
	db     *sql.DB
	config Config
	wg     sync.WaitGroup

	//Main data stream to pull data from to ingest
	stream chan *types.Chunk
	//If any connect fail, sql will roll back and we need to re-ingest the data
	failStream chan *types.Chunk

	totalRows *int64

	//cache items
	stmtsStr   string
	stmtsBatch map[int]string
}

// New is use to create a new Ingest structure
// The number of connection make to the database will scale up from config.minConnection to config.maxConnections,
//		The minimum connection to keep open is set by config.minConnection, this is use as the baseline to maintain a
//		consistent stream into the database
//		if config.maxConnections is set to -1, scale upto as many connection is needed to consume the data stream
func New(db *sql.DB, config Config) *Ingest {
	if config.MinConnection <= 0 {
		config.MinConnection = 1
	}
	//

	//if config.MaxConnection == 0 {
	//	config.MaxConnections = 10
	//}

	totalRow := int64(0)
	return &Ingest{
		db:         db,
		config:     config,
		stmtsBatch: map[int]string{},
		totalRows:  &totalRow,
	}
}

func (i *Ingest) AddChunk(c *types.Chunk) {
	i.stream <- c
}

func (i *Ingest) GetStream() chan *types.Chunk {
	return i.stream
}

func (i *Ingest) GetTotalRows() int64 {
	return *i.totalRows
}

func (i *Ingest) batchInsert() {
	defer i.wg.Done()

	tx, err := i.db.Begin()
	if err != nil {
		panic(err)
	}
	uncommitRow := 0
	var stmt *sql.Stmt
	lastPrepLength := 0
	var stmtStr string
	var chunk *types.Chunk
	var ok bool
	for {
		select {
		case chunk, ok = <-i.failStream:
			if !ok {
				return
			}
			break
		case chunk, ok = <-i.stream:
			if !ok {
				//The stream is close, close the transaction
				if stmt != nil {
					if err := stmt.Close(); err != nil {
						panic(err)
					}
				}
				if tx != nil {
					if err := tx.Commit(); err != nil {
						panic(err)
					}
				}
				return
			}
			break
		}

		if len(chunk.Data)%i.config.NumberOfColumns != 0 {
			go func(c *types.Chunk) {
				if c.Callback != nil {
					c.Callback(errors.New("the number of columns in the data is not correct"))
				}
			}(chunk)
			continue
		}
		numberOfRow := len(chunk.Data) / i.config.NumberOfColumns
		uncommitRow += numberOfRow
		if lastPrepLength != numberOfRow {
			stmtStr, ok = i.stmtsBatch[numberOfRow]
			if !ok {
				stmtStr = i.createBatchStmt(numberOfRow)
				i.stmtsBatch[numberOfRow] = stmtStr
			}
			if stmt != nil {
				if err := stmt.Close(); err != nil {
					//TODO handle error better
					fmt.Println(err)
					chunk.Retry++
					i.failStream <- chunk
					//Let another thread pick this up. We do not want to spam ourself with retries
					time.Sleep(time.Second)
					continue
				}
			}
			stmt, err = tx.Prepare(stmtStr)
			if err != nil {
				//TODO handle error better
				fmt.Println(err)
				chunk.Retry++
				i.failStream <- chunk
				time.Sleep(time.Second)
				continue
			}
			lastPrepLength = numberOfRow
		}
		_, err := stmt.Exec(chunk.Data...)
		if err != nil {
			i.failStream <- chunk
			//TODO handle uncommitted rows when database transaction logs fill up
			if uncommitRow > 0 {
				if !strings.Contains(err.Error(), "The transaction log for the database is full") {
					fmt.Println(err)
				}
				if err := tx.Commit(); err != nil {
					if !strings.Contains(err.Error(), "transaction has already been committed or rolled back") {
						panic(err)
					}
				}
			}
		}
		atomic.AddInt64(i.totalRows, int64(numberOfRow))
		if uncommitRow > i.config.CommitSize {
			uncommitRow = 0
			//TODO handle each of these failures better
			//Unlike a single chuck of files, we need to ensure we either commit or
			// rollback the transaction and let the upstream process handle the retry
			if err := stmt.Close(); err != nil {
				panic(err)
			}
			stmt = nil
			if err := tx.Commit(); err != nil {
				panic(err)
			}
			if tx, err = i.db.Begin(); err != nil {
				panic(err)
			}
		}
		go chunk.Callback(nil)
	}
}

func (in *Ingest) createBatchStmt(numberOfRow int) string {
	if len(in.stmtsStr) == 0 {
		var oneSet strings.Builder
		size := in.config.NumberOfColumns*2 + 5
		oneSet.Grow(size)
		//In Golang, there is a cost to go from string to bytes.
		oneSet.Write([]byte{'(', ' ', '?'})
		for idx := 1; idx < in.config.NumberOfColumns; idx++ {
			oneSet.Write([]byte{',', '?'})
		}
		oneSet.Write([]byte{')'})
		in.stmtsStr = oneSet.String()
	}

	var value strings.Builder
	value.Grow(numberOfRow * len(in.stmtsStr))
	for idx := 0; idx < numberOfRow; idx++ {
		value.WriteString(in.stmtsStr)
	}
	return value.String()
}

func (i *Ingest) Start() error {
	if i.stream != nil {
		return errors.New("ingest already started")
	}
	//Buffer the stream to prevent blocking
	i.stream = make(chan *types.Chunk, i.config.MinConnection*10)
	i.failStream = make(chan *types.Chunk, i.config.MinConnection*5)

	for idx := 0; idx < i.config.MinConnection; idx++ {
		i.wg.Add(1)
		go i.batchInsert()
	}
	return nil
}

func (i *Ingest) Stop() {
	close(i.stream)
	close(i.failStream)
}

func (i *Ingest) Wait() {
	i.wg.Wait()
}
