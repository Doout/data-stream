package ingest

import (
	"errors"
	"fmt"
	"github.com/doout/data-stream/pkg/types"
	"io"
	"os"
	"strconv"
	"time"
)

// Rate Time the ingest rate is being processed
type Rate struct {
	injest          types.IngestInterface
	lastTotalRows   int64
	lastTotalChunks int64

	startTime time.Time
	duration  time.Duration
	lastTime  time.Time
	out       io.Writer

	cancelChan chan struct{}
}

func NewRate(injest types.IngestInterface) *Rate {
	return &Rate{
		injest:   injest,
		duration: time.Second,
		out:      os.Stdout,
	}
}

func (r *Rate) Start() error {
	if r.cancelChan != nil {
		return errors.New("rate already started")
	}
	r.cancelChan = make(chan struct{})
	go r.run()
	return nil
}

func (r *Rate) Stop() {
	r.cancelChan <- struct{}{}
}

func (r *Rate) run() {
	r.startTime = time.Now()
	r.lastTime = time.Now()
	for {
		select {
		case <-r.cancelChan:
			r.cancelChan = nil
			return
		default:
		}
		time.Sleep(r.duration)
		totalRowInjest := r.injest.GetTotalRows()
		totalChunkInjest := r.injest.GetTotalChunks()

		//Set new time right away once we get totalRows
		end := time.Now()
		diffTime := end.Sub(r.lastTime)
		diffRows := totalRowInjest - r.lastTotalRows
		diffChunks := totalChunkInjest - r.lastTotalChunks

		r.lastTotalChunks = totalChunkInjest
		r.lastTotalRows = totalRowInjest
		//Number of Row per duration
		rowPerDur := float64(diffRows) / diffTime.Seconds()
		chunkPerDur := float64(diffChunks) / diffTime.Seconds()
		fmt.Fprintf(r.out, "Total Row : %d \t Row/s : %s\t Chunk/s %s \tTime Elapsed : %s\n", totalRowInjest, formatQuantity(rowPerDur), formatQuantity(chunkPerDur), end.Sub(r.startTime))
		r.lastTime = end
	}
}

var quantityUnit = []string{"", "Thousand", "Million", "Billion", "Trillion"}

func formatQuantity(quantity float64) string {
	index := 0
	q := float64(quantity)
	for q > 1000 {
		index++
		q /= 1000
	}
	if index == 0 {
		return strconv.FormatFloat(q, 'f', 5, 64)
	}
	return strconv.FormatFloat(q, 'f', 5, 64) + " " + quantityUnit[index]
}
