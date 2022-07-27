package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/gomodule/redigo/redis"
	"github.com/gorilla/websocket"
	uuid "github.com/satori/go.uuid"
)

const (
	SUBSCRIBE   = "subscribe"
	UNSUBSCRIBE = "unsubscribe"
)

type Client struct {
	Id         string
	Connection *websocket.Conn
}

type WsMessage struct {
	Action string `json:"action"`
	Topic  string `json:"topic"`
}

type Subscription struct {
	Topic  string
	Client Client
}

type PubSub struct {
	Subscriptions []Subscription
}

type publishMessage struct {
	Message json.RawMessage `json:"message"`
	Topic   string          `json:"topic"`
}

type subscribeMessage struct {
	client Client
	topic  string
}

type unsubscribeMessage struct {
	client Client
	topic  string
}

var pubChannel = make(chan publishMessage)
var subChannel = make(chan subscribeMessage)
var unsubChannel = make(chan unsubscribeMessage)

func (ps *PubSub) subscribe(client Client, topic string) {

	for _, subscription := range ps.Subscriptions {
		if subscription.Client.Id == client.Id && subscription.Topic == topic {
			return
		}
	}

	newSubscription := Subscription{
		Topic:  topic,
		Client: client,
	}

	ps.Subscriptions = append(ps.Subscriptions, newSubscription)

}

func (ps *PubSub) unsubscribe(client Client, topic string) {

	for index, sub := range ps.Subscriptions {

		if sub.Client.Id == client.Id && sub.Topic == topic {
			// remove the subscription

			ps.Subscriptions = append(ps.Subscriptions[:index], ps.Subscriptions[index+1:]...)

		}
	}

}

func (ps *PubSub) Run() {

	for {

		select {

		case m := <-subChannel:

			ps.subscribe(m.client, m.topic)

		case m := <-unsubChannel:

			ps.unsubscribe(m.client, m.topic)

		case m := <-pubChannel:

			message, err := json.Marshal(m)
			if err != nil {
				log.Println("Error marshalling publish message")
				break
			}

			wg := &sync.WaitGroup{}

			for _, subscription := range ps.Subscriptions {
				if isSubTopic(subscription.Topic, m.Topic) {
					wg.Add(1)
					go func(subscription Subscription) {
						defer wg.Done()

						err2 := subscription.Client.Connection.WriteMessage(websocket.TextMessage, message)
						if err2 != nil {
							log.Printf("Unable to send message %s to client %s \n", message, subscription.Client.Id)
							return
						}

						log.Printf("Message: %s sent to client %s \n", message, subscription.Client.Id)
					}(subscription)
				}
			}

			wg.Wait()
			log.Printf("Message: %s sent to all subscribers", message)
		}

	}

}

func HandleReceiveMessage(client Client, payload []byte) {

	m := WsMessage{}

	err := json.Unmarshal(payload, &m)
	if err != nil {
		log.Println("Incorrectly formatted payload from client ", client.Id)
		client.Connection.WriteMessage(websocket.TextMessage, []byte("Incorrectly formatted payload"))
		return
	}

	if m.Action != SUBSCRIBE && m.Action != UNSUBSCRIBE {
		log.Println("Incorrect action ", m.Action)
		client.Connection.WriteMessage(websocket.TextMessage, []byte("Incorrect action only subscribe and unsubscribe are supported from /ws endpoint"))
		return
	}

	if !isValidSubscribeTopic(m) {
		log.Println("Incorrectly formatted topic from client ", client.Id)
		client.Connection.WriteMessage(websocket.TextMessage, []byte("Incorrectly formatted topic"))
		return
	}

	switch m.Action {

	case SUBSCRIBE:

		log.Println("new subscriber to topic", m.Topic, client.Id)

		subChannel <- subscribeMessage{client: client, topic: m.Topic}

	case UNSUBSCRIBE:

		log.Println("Client wants to unsubscribe the topic", m.Topic, client.Id)

		unsubChannel <- unsubscribeMessage{client: client, topic: m.Topic}

	default:

		log.Println("Unknown action type from client ", m.Action, client.Id)

	}

}

func isSubTopic(main, subTopic string) bool {
	if main == subTopic {
		return true
	}
	main_ := strings.Split(main, "|")
	subTopic_ := strings.Split(subTopic, "|")

	if main_[0] == "*" {
		// handles case where main is *|*|*
		return true
	} else if main_[0] == subTopic_[0] && main_[1] == "*" {
		// handles case where main is office|*|*
		return true
	} else if main_[0] == subTopic_[0] && main_[1] == subTopic_[1] && main_[2] == "*" {
		// handles case where main is office|bucket|*
		return true
	}

	return false
}

func isValidSubscribeTopic(m WsMessage) bool {
	// office|bucket|filename
	switch m.Action {
	case SUBSCRIBE, UNSUBSCRIBE:
		pipe_count := strings.Count(m.Topic, "|")
		return pipe_count == 2
	default:
		return false
	}
}

func isValidPublishTopic(m publishMessage) bool {
	// office|bucket|filename
	topic_slice := strings.Split(m.Topic, "|")
	return len(topic_slice) == 3 && topic_slice[0] != "*" && topic_slice[1] != "*" && topic_slice[2] != "*"
}

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle: 20,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}

var redis_pool = newPool()

func checkAuthCookie(r *http.Request) bool {
	client := redis_pool.Get()
	defer client.Close()

	cookie, err := r.Cookie("set-cookie")
	if err != nil {
		return false
	}
	cookie_str := cookie.String()
	if cookie_str == "" {
		return false
	}

	value, err := client.Do("GET", cookie_str)
	if err != nil {
		return false
	}

	return value != nil
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     checkAuthCookie,
}

func websocketHandler(w http.ResponseWriter, r *http.Request) {

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	client := Client{
		Id:         uuid.NewV4().String(),
		Connection: conn,
	}

	log.Printf("New Client %s is connected\n", client.Id)

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Println("Something went wrong", err)

			return
		}
		if messageType != websocket.TextMessage {
			log.Println("Message type is not text")
			return
		}

		HandleReceiveMessage(client, p)

	}

}

func publishHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	m := publishMessage{}

	err := json.NewDecoder(r.Body).Decode(&m)
	if err != nil {
		log.Println("Error decoding message")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if !isValidPublishTopic(m) {
		log.Println("Incorrectly formatted topic")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Printf("publish new message on topic %s - %s\n", m.Topic, m.Message)

	pubChannel <- m

}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	pubsub := &PubSub{}

	mux := http.NewServeMux()

	mux.HandleFunc("/ws", websocketHandler)
	mux.HandleFunc("/publish", publishHandler)
	srv := &http.Server{
		Addr:    ":3005",
		Handler: mux,
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		err := srv.ListenAndServe()
		if err != nil {
			log.Println(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		pubsub.Run()
	}()

	log.Println("Server is running: http://localhost:3005")
	wg.Wait()
}
