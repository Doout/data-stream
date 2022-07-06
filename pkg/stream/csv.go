package stream

import (
	"encoding/csv"
	"github.com/doout/data-stream/pkg/types"
	"io"
	"os"
)

type CsvStream struct {
	FileName   string
	Delimiter  string
	BatchSize  int
	PreProcess func(values []string, dist []any)
	file       *os.File
	csv        *csv.Reader
	ColumnSize int
}

func NullIfEmpty(values []string, dist []any) {
	for idx, value := range values {
		if value == "" {
			dist[idx] = "NULL"
			continue
		}
		dist[idx] = value
	}
}

func NewCsvStream(fileName, delimiter string, batchSize, columnSize int) *CsvStream {
	return &CsvStream{
		FileName:   fileName,
		Delimiter:  delimiter,
		BatchSize:  batchSize,
		ColumnSize: columnSize,
		PreProcess: NullIfEmpty,
	}
}

func (c *CsvStream) Open() error {
	f, err := os.Open(c.FileName)
	if err != nil {
		return err
	}
	c.file = f
	c.csv = csv.NewReader(f)
	c.csv.FieldsPerRecord = c.ColumnSize
	c.csv.ReuseRecord = true
	return nil
}

func (c *CsvStream) Read() (*types.Chunk, error) {
	chunk := &types.Chunk{
		Data: make([]interface{}, 0, c.BatchSize*c.ColumnSize),
	}
	readRow := c.BatchSize
	c.csv.ReuseRecord = true
	arr := make([]any, c.ColumnSize)
	for readRow > 0 {
		record, err := c.csv.Read()
		if err != nil {
			if err == io.EOF {
				return chunk, io.EOF
			}
			return nil, err
		}
		if c.PreProcess != nil {
			c.PreProcess(record, arr)
		} else {
			NullIfEmpty(record, arr)
		}
		chunk.Data = append(chunk.Data, arr...)
		readRow--
	}
	return chunk, nil
}

func (c *CsvStream) Close() error {
	return c.file.Close()
}
