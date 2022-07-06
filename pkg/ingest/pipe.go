package ingest

import (
	"errors"
	"github.com/doout/data-stream/pkg/types"
	"io"
	"sync"
)

//Pipe a data stream into the ingest stream
type Pipe struct {
	ds         types.DataStream
	ingest     types.IngestInterface
	wg         sync.WaitGroup
	jobs       chan struct{}
	cancelChan chan struct{}
}

func NewPipe(ds types.DataStream, ingest types.IngestInterface) *Pipe {
	return &Pipe{ds: ds, ingest: ingest}
}

func NewPipeWithLimit(ds types.DataStream, ingest types.IngestInterface, limit chan struct{}) *Pipe {
	return &Pipe{ds: ds, ingest: ingest, jobs: limit}
}

func (p *Pipe) Start() error {
	if p.cancelChan != nil {
		return errors.New("pipe already started")
	}
	if p.ds == nil {
		return errors.New("data stream is nil")
	}
	if p.ingest == nil {
		return errors.New("data stream is nil")
	}
	if p.jobs != nil {
		<-p.jobs
		defer func() { p.jobs <- struct{}{} }()
	}
	p.wg.Add(1)
	go p.run()
	return nil
}

func (p *Pipe) Stop() {
	p.cancelChan <- struct{}{}
}

func (p *Pipe) Wait() {
	p.wg.Wait()
}

func (p *Pipe) run() {
	defer p.wg.Done()
	for {
		select {
		case <-p.cancelChan:
			p.cancelChan = nil
			return
		default:
		}
		chunk, err := p.ds.Read()
		if err != nil {
			if err == io.EOF {
				if len(chunk.Data) > 0 {
					p.ingest.AddChunk(chunk)
				}
				return
			}
			println(err.Error())
			continue
		}
		p.ingest.AddChunk(chunk)
	}
}
