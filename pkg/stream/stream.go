package stream

import (
	"errors"
	"github.com/doout/data-stream/pkg/types"
)

type Stream struct {
	ds     types.DataStream
	ingest types.IngestInterface

	cancelChan chan struct{}
}

func NewStream(ds types.DataStream, ingest types.IngestInterface) *Stream {
	return &Stream{ds: ds, ingest: ingest}
}

func (s *Stream) Start() error {
	if s.cancelChan != nil {
		return errors.New("stream already started")
	}
	s.cancelChan = make(chan struct{})
	err := s.ds.Open()
	if err != nil {
		return err
	}
	go s.run()
	return nil
}

func (s *Stream) Stop() {
	s.cancelChan <- struct{}{}
}

func (s *Stream) run() {
	defer s.ds.Close()
	for {
		select {
		case <-s.cancelChan:
			s.cancelChan = nil
			return
		default:
		}
		chunk, err := s.ds.Read()
		if err != nil {
			println(err.Error())
			//TODO handle errors
			continue
		}
		s.ingest.AddChunk(chunk)
	}
}
