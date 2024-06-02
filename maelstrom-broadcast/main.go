package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	n := maelstrom.NewNode()
	mu := sync.RWMutex{}
	wg := sync.WaitGroup{}
	reads := make([]int, 0)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := make(map[string]any)

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		log.Print("BODY HERE", body)

		val := int(body["message"].(float64))

		wg.Add(1)

		go func(x int) {
			mu.Lock()
			reads = append(reads, x)
			mu.Unlock()
			wg.Done()
		}(val)

		wg.Wait()

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Fatal(err)
		}

		r := make([]int, 0)

		for _, v := range reads {
			mu.RLock()
			r = append(r, v)
			mu.RUnlock()
		}

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": r,
		})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		body := make(map[string]interface{})

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			log.Fatal(err)
		}

		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
