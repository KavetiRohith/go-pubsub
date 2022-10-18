package main

import (
	"encoding/json"
	"time"
)

type WsMessage struct {
	Action  string           `json:"action"`
	Topic   string           `json:"topic"`
	Message *json.RawMessage `json:"message"`
}

type PublishMessage struct {
	Message *json.RawMessage `json:"message"`
	Topic   string           `json:"topic"`
	Time    time.Time        `json:"message_received_at"`
}

type SubscribeMessage struct {
	client *Client
	topic  string
}

type UnsubscribeMessage struct {
	client *Client
	topic  string
}
