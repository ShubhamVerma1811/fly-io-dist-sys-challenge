package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {

	n := maelstrom.NewNode()
	reads := make([]int, 0)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		body := make(map[string]any)

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		val := int(body["message"].(float64))

		reads = append(reads, val)

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
			"messages": reads,
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
