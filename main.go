package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync/atomic"
	"time"
)

func main() {

	// Echo
	n := maelstrom.NewNode()
	n.Handle("echo", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "echo_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Unique ID Generation
	nodeCounterMap := make(map[string]int64)
	n.Handle("generate", func(msg maelstrom.Message) error {
		counter := int64(1)
		val, ok := nodeCounterMap[n.ID()]
		if ok {
			counter = val
		}
		atomic.AddInt64(&counter, 1)
		nodeCounterMap[n.ID()] = counter
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type to return back.
		body["type"] = "generate_ok"

		intNodeId := 0
		for _, ch := range n.ID() {
			intNodeId += int(ch)
		}

		body["id"] = int64(intNodeId) + counter + time.Now().UnixNano()
		return n.Reply(msg, body)
	})
	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
