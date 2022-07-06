package ingest

import (
	"github.com/doout/data-stream/pkg/types"
	"sync"
	"sync/atomic"
)

//TODO Create an embedded database to test out ingest

// Ingest Create a sinkhole for the ingest data.
type IngestSinkhole struct {
	config     Config
	stream     chan *types.Chunk
	totalRows  *int64
	totalChunk *int64

	wg sync.WaitGroup
}

func NewIngestSinkhole(config Config) *IngestSinkhole {
	totalRows := int64(0)
	totalChunk := int64(0)
	return &IngestSinkhole{config: config, totalRows: &totalRows, totalChunk: &totalChunk}
}

func (i *IngestSinkhole) AddChunk(chunk *types.Chunk) {
	i.stream <- chunk
}

func (i *IngestSinkhole) GetTotalRows() int64 {
	return *i.totalRows
}

func (i *IngestSinkhole) GetStream() chan *types.Chunk {
	return i.stream
}
func (i *IngestSinkhole) Start() error {
	i.stream = make(chan *types.Chunk, 100)
	i.wg.Add(1)
	go i.run()
	return nil
}

func (i *IngestSinkhole) Stop() {
	close(i.stream)
}

func (i *IngestSinkhole) Wait() {
	i.wg.Wait()
}

func (i *IngestSinkhole) run() {
	defer i.wg.Done()
	for {
		chunk, ok := <-i.stream
		if !ok {
			return
		}
		numberOfRow := len(chunk.Data) / i.config.NumberOfColumns
		atomic.AddInt64(i.totalRows, int64(numberOfRow))
		atomic.AddInt64(i.totalChunk, int64(1))
	}
}

func (i *IngestSinkhole) GetTotalChunks() int64 {
	return *i.totalChunk
}
