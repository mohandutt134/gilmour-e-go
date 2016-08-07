package main

import (
	"../../backends"
	G "github.com/gilmour-libs/gilmour-e-go"
	"log"
	"time"
)

func echoEngine(master string, sentinels []string) *G.Gilmour {
	redis := backends.MakeRedisSentinel(master, "", sentinels)
	engine := G.Get(redis)
	return engine
}

// func ExecuteRequest(request *G.RequestComposer, c chan int, count int) {
// 	req_msg := G.NewMessage()
// 	resp, err := request.Execute(req_msg)

// 	if resp == nil {
// 		log.Println("nil response due to ", err)
// 	}

// 	msg := resp.Next()

// 	var data string
// 	msg.GetData(&data)
// 	log.Println(data)
// 	c <- 1
// }

func main() {
	sentinels := []string{":16380", ":16381", ":16382"}
	engine := echoEngine("mymaster", sentinels)
	engine.Start()

	request := engine.NewRequest("test.handler.one")
	c := make(chan int)
	total := 0

	final := time.Now().Add(2 * time.Second)
	for i := 0; time.Now().Before(final); i += 2 {
		// 	go ExecuteRequest(request, c, i)
		go func() {
			req_msg := G.NewMessage()
			resp, err := request.Execute(req_msg)

			if resp == nil {
				log.Println("nil response due to ", err)
			} else {

				msg := resp.Next()

				var data string
				msg.GetData(&data)
				log.Println(data)
			}
		}()
		select {
		case count := <-c:
			total += count
		default:
			total = total
		}
	}
	log.Println("Total throughput = ", total, " per second")
}
