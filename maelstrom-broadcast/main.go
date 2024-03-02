package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Read struct {
	messages []int
	sync.RWMutex
}

func main() {

	n := maelstrom.NewNode()
	// reads := Read{
	// }
	reads := Read{
		messages: make([]int, 0),
	}

	wg := sync.WaitGroup{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := make(map[string]any)

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		wg.Add(1)
		go func(read int) {
			reads.Lock()
			defer reads.Unlock()
			reads.messages = append(reads.messages, read)
		}(int(body["message"].(float64)))

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

		return n.Reply(msg, map[string]any{
			"type":     "read_ok",
			"messages": getAllIds(&Read{messages: reads.messages}),
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

func getAllIds(msgs *Read) []int {
	msgs.RLock()
	defer msgs.RUnlock()

	return msgs.messages

}
