package types

import (
	"io"
)

type DataStream interface {
	Open() error
	Read() (*Chunk, error)
	io.Closer
}
