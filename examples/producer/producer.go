package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/vincent3i/nsqueue/producer"
)

var (
	amount   = flag.Int("amount", 20, "Amount of messages to produce every 100 ms")
	nsqdAddr = flag.String("nsqd", "115.28.220.146:4150", "nsqd tcp address")
)

func main() {
	flag.Parse()
	producer.Connect(*nsqdAddr)

	for _ = range time.Tick(100 * time.Millisecond) {
		fmt.Println("Ping...")
		for i := 0; i < *amount; i++ {
			body, _ := time.Now().MarshalBinary()
			producer.PublishAsync("latency-test", body, nil)
		}
	}
}
