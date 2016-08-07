package main

import (
	"../../backends"
	"fmt"
	// G "github.com/gilmour-libs/gilmour-e-go"
	G "../../"
	"log"
	"sync"
	"time"
)

func echoEngine(master string, sentinels []string) *G.Gilmour {
	redis := backends.MakeRedisSentinel(master, "", sentinels)
	engine := G.Get(redis)
	engine.EnableRetry(G.RetryConf{
		Timeout:    5 * time.Second,        // in seconds
		CheckEvery: 100 * time.Millisecond, // in milliseconds
	})
	return engine
}

//Fetch a remote file from the URL received in Request.
func fetchReply(g *G.Gilmour) func(req *G.Request, resp *G.Message) {
	return func(req *G.Request, resp *G.Message) {
		var num int
		if err := req.Data(&num); err != nil {
			panic(err)
		}

		//Send back the contents.
		log.Println("Responding to ", num)
		resp.SetData(fmt.Sprint("Response of ", num))
	}
}

//Bind all service endpoints to their topics.
func bindListener(g *G.Gilmour) {
	g.ReplyTo("test.handler.one", fetchReply(g), nil)
}

func main() {
	sentinels := []string{":16380", ":16381", ":16382"}
	engine := echoEngine("mymaster", sentinels)
	bindListener(engine)
	engine.Start()
	var wg sync.WaitGroup
	wg.Add(1)
	wg.Wait()
}
