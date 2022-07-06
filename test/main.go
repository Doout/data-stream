package main

import (
	"fmt"
	"github.com/doout/data-stream/pkg/ingest"
	"github.com/doout/data-stream/pkg/stream"
	"os"
	"time"
)

func main() {
	home, _ := os.UserHomeDir()
	csv := stream.NewCsvStream(home+"/Downloads/archive/2019-Nov.csv", ",", 250, 9)
	csv.Open()
	//sinkhole := stream.NewSinkHole(csv)
	//sinkhole.Start()
	//sinkhole.Wait()
	config := ingest.Config{MinConnection: 100, NumberOfColumns: 9}
	in := ingest.NewIngestSinkhole(config)
	pipe := ingest.NewPipe(csv, in)
	rate := ingest.NewRate(in)

	in.Start()
	pipe.Start()
	rate.Start()
	pipe.Wait()

	//ensure the last chunk is processed
	time.Sleep(time.Second * 5)
	//total line 67 501 980
	//           67501750
	fmt.Println(in.GetTotalRows())
	//event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
}
