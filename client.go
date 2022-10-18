package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
)

const (
	SUBSCRIBE   = "subscribe"
	UNSUBSCRIBE = "unsubscribe"
	PUBLISH     = "publish"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 512
)

var (
	newline = []byte{'\n'}
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

// Client is a middleman between the websocket connection and the hub.
type Client struct {
	ID string

	hub *Hub

	// The websocket connection.
	conn *websocket.Conn

	// Buffered channel of outbound messages.
	send chan PublishMessage

	// chan used to send info about malformed topics and status messages to the client
	infoChan chan string

	//	unbuffered channel used show a connection is closed
	disconnect chan struct{}
}

// readPump pumps messages from the websocket connection to the hub.
//
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (c *Client) readPump(wg *sync.WaitGroup) {
	defer func() {
		// unregister the client
		select {
		case c.hub.unregister <- *c:
		// ignore unregister when exit is triggered
		case <-c.hub.exitchan:
		}
		c.conn.Close()
		log.Println("stopped readPump for ", c.ID)
		wg.Done()
	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
READLOOP:
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v\n", err)
			}
			break READLOOP
		}

		m := WsMessage{}

		if err = json.Unmarshal(message, &m); err != nil {
			log.Printf("Error decoding json from client %v: %v\n", c.ID, err)
			c.infoChan <- "Invalid json received"
			continue READLOOP
		}

	SWITCH:
		switch m.Action {
		case SUBSCRIBE:
			if !isValidSubscribeTopic(&m.Topic) {
				log.Printf("Incorrectly formatted subscribe topic: %v from client: %v\n", m.Topic, c.ID)
				c.infoChan <- fmt.Sprintf("Incorrectly formatted subscribe topic: %v from client: %v\n", m.Topic, c.ID)
				break SWITCH
			}

			select {
			case c.hub.subscribe <- SubscribeMessage{c, m.Topic}:
			case <-c.disconnect:
				log.Printf("Ignoring subscribe to topic: %v from client: %v", m.Topic, c.ID)
				return
			}

		case UNSUBSCRIBE:
			if !isValidSubscribeTopic(&m.Topic) {
				log.Printf("Incorrectly formatted unsubscribe topic: %v from client: %v\n", m.Topic, c.ID)
				c.infoChan <- fmt.Sprintf("Incorrectly formatted unsubscribe topic: %v from client: %v\n", m.Topic, c.ID)
				break SWITCH
			}

			select {
			case c.hub.unsubscribe <- UnsubscribeMessage{c, m.Topic}:
			case <-c.disconnect:
				log.Printf("Ignoring unsubscribe to topic: %v from client: %v", m.Topic, c.ID)
				return
			}

		case PUBLISH:
			if !isValidPublishTopic(&m.Topic) {
				log.Printf("Incorrectly formatted publish topic: %v from client: %v\n", m.Topic, c.ID)
				c.infoChan <- fmt.Sprintf("Incorrectly formatted publish topic: %v from client: %v\n", m.Topic, c.ID)
				break SWITCH
			}

			select {
			case c.hub.broadcast <- PublishMessage{Message: m.Message, Topic: m.Topic, Time: time.Now()}:
				log.Printf("publish new message on topic %s - %s\n", m.Topic, m.Message)
				c.infoChan <- fmt.Sprintf("publish on topic %v initiated\n", m.Topic)
			case <-c.disconnect:
				log.Printf("Ignoring publish to topic: %v with message %v from client: %v", m.Topic, m.Message, c.ID)
				return
			}

		default:

			log.Printf("Unknown action %v drom client %v\n", m.Action, c.ID)
		}
	}
}

// writePump pumps messages from the hub to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (c *Client) writePump(wg *sync.WaitGroup) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
		close(c.disconnect)
		log.Println("stopped writePump for ", c.ID)
		wg.Done()
	}()
	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				)
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				log.Printf("Write to Client with Id %v failed closing, error %v\n", c.ID, err)
				return
			}
			messageJson, err := json.Marshal(message)
			if err != nil {
				log.Println("Error marshalling publish message, error: ", err)
				break
			}
			w.Write(messageJson)

			// Add queued publish messages to the current websocket message.
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(newline)
				message := <-c.send
				messageJson, err := json.Marshal(message)
				if err != nil {
					log.Println("Error marshalling publish message", err, message)
					continue
				}
				w.Write(messageJson)
			}

			if err := w.Close(); err != nil {
				log.Println("error closing writer", err)
				return
			}

		case message, ok := <-c.infoChan:
			if ok {
				c.conn.SetWriteDeadline(time.Now().Add(writeWait))
				err := c.conn.WriteMessage(websocket.TextMessage, []byte(message))
				if err != nil {
					log.Printf("Write to Client with Id %v failed closing, error %v\n", c.ID, err)
					return
				}
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("Ping to Client with Id %v failed closing, error %v\n", c.ID, err)
				return
			}
		}
	}
}

// serveWs handles websocket requests from the peer.
func serveWs(redisPool *redis.Pool, hub *Hub, w http.ResponseWriter, r *http.Request, wg *sync.WaitGroup) {
	token, err := GetAuthToken(redisPool, r)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	// client := &Client{ID: uuid.NewV4().String(), hub: hub, conn: conn, send: make(chan *PublishMessage, 256), infoChan: make(chan string), disconnect: make(chan struct{})}
	client := Client{ID: token, hub: hub, conn: conn, send: make(chan PublishMessage, 256), infoChan: make(chan string), disconnect: make(chan struct{})}

	client.hub.register <- client

	// Allow collection of memory referenced by the caller by doing all work in
	// new goroutines.
	wg.Add(2)
	go client.writePump(wg)
	go client.readPump(wg)
}

// servePublish handles publish requests from /publish endpoint.
func servePublish(hub *Hub, w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	m := PublishMessage{}

	err := json.NewDecoder(r.Body).Decode(&m)
	if err != nil {
		log.Println("Error decoding message")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	m.Time = time.Now()

	if strings.TrimSpace(string(*m.Message)) == "" || strings.TrimSpace(m.Topic) == "" {
		log.Println("Neither message nor topic can be Empty")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Neither message nor topic can be Empty"))
		return
	}

	if !isValidPublishTopic(&m.Topic) {
		log.Println("Incorrectly formatted topic")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Incorrectly formated publish topic"))
		return
	}

	log.Printf("publish new message on topic %s - %s\n", m.Topic, m.Message)

	hub.broadcast <- m
}
