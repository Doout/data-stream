package types

type IngestInterface interface {
	AddChunk(chunk *Chunk)
	GetTotalRows() int64
	GetTotalChunks() int64

	GetStream() chan *Chunk

	Start() error
	Stop()
	Wait()
}

type Chunk struct {
	StreamId int64
	Index    int64
	Data     []any
	Callback func(error)

	Retry int
}
