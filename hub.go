package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"time"
)

const writeWaitChan = (12 * writeWait) / 10

// Hub maintains the set of active clients and broadcasts messages to the
// clients.
type Hub struct {
	// Registered clients.
	clients map[string]Client

	// map of  user id to subscriptions
	subscriptionsMap SubscriptionsMap

	// Inbound messages from the clients.
	broadcast chan PublishMessage

	// Register requests from the clients.
	register chan Client

	// Unregister requests from clients.
	unregister chan Client

	// Subscribe requests from clients.
	subscribe chan SubscribeMessage

	// Unsubscribe requests from clients.
	unsubscribe chan UnsubscribeMessage

	exitchan chan struct{}
}

func newHub(prevSubscriptions SubscriptionsMap, exitchan chan struct{}) *Hub {
	return &Hub{
		broadcast:        make(chan PublishMessage),
		register:         make(chan Client),
		unregister:       make(chan Client),
		subscribe:        make(chan SubscribeMessage),
		unsubscribe:      make(chan UnsubscribeMessage),
		clients:          make(map[string]Client),
		subscriptionsMap: prevSubscriptions,
		exitchan:         exitchan,
	}
}

func (h *Hub) run() {
	defer h.DumpSubscriptionsToFile(
		path.Join(*pubsubDataDirPath, *subscriptionsFile),
	)

	for {
		select {
		case client := <-h.register:
			h.clients[client.ID] = client

			if h.subscriptionsMap[client.ID] == nil {
				h.subscriptionsMap[client.ID] = map[string]struct{}{}
			}

			log.Printf("Client %v connected with IP address %v\n", client.ID, client.conn.RemoteAddr())
			pendingPublishMessages, err := readPendingPublishMessages(client.ID)
			if err != nil {
				log.Println("unable to read pending publish messages of client ", client.ID, err)
				continue
			}

		PUBLISHLOOP:
			for index := range pendingPublishMessages {
				writeWaitTimer := time.NewTimer(writeWaitChan)
				select {
				case client.send <- pendingPublishMessages[index]:
					if !writeWaitTimer.Stop() {
						// if the timer has been stopped then read from the channel.
						<-writeWaitTimer.C
					}

				// handle case where the client can't keep up with
				// the rate of messages generated
				case <-writeWaitTimer.C:
					close(client.send)
					delete(h.clients, client.ID)
					savePublishMessagesToFile(client.ID, pendingPublishMessages[index:])
					log.Printf("Client %v cannot keepup with the message rate disconnecting and dumping the remaining messages to file %v \n", client.ID, client.ID+".ndjson")
					break PUBLISHLOOP
				}
			}

		case client := <-h.unregister:
			if _, ok := h.clients[client.ID]; ok {
				delete(h.clients, client.ID)
				close(client.send)
			}
			log.Printf("Client %v disconnected\n", client.ID)

		case message := <-h.subscribe:
			subs := h.subscriptionsMap[message.client.ID]
			subs[message.topic] = struct{}{}
			h.subscriptionsMap[message.client.ID] = subs

			log.Printf("Client %v subscribed to topic %v\n", message.client.ID, message.topic)

			message.client.infoChan <- fmt.Sprintf("subscription to topic %v successful", message.topic)

		case message := <-h.unsubscribe:
			subs := h.subscriptionsMap[message.client.ID]
			delete(subs, message.topic)
			h.subscriptionsMap[message.client.ID] = subs

			log.Printf("Client %v unsubscribed from topic %v\n", message.client.ID, message.topic)
			message.client.infoChan <- fmt.Sprintf("unsubscribe from topic %v successful", message.topic)

		case publishMessage := <-h.broadcast:
		ClientListLoop:
			for clientID, subscriptionTopics := range h.subscriptionsMap {
				for subscriptionTopic := range subscriptionTopics {
					// check whether subscriptionTopic is an instance of publishTopic
					if isSubTopic(subscriptionTopic, publishMessage.Topic) {
						client, isConnected := h.clients[clientID]

						if isConnected {
							writeWaitTimer := time.NewTimer(writeWaitChan)

							select {
							case client.send <- publishMessage:
								// stop the timer once the message is sent.
								if !writeWaitTimer.Stop() {
									// if the timer has been stopped then read from the channel.
									<-writeWaitTimer.C
								}

							// handle case where the client can't keep up with
							// the rate of messages generated
							case <-writeWaitTimer.C:
								close(client.send)
								delete(h.clients, clientID)
								savePublishMessageToFile(clientID, publishMessage)
								log.Printf("write to client %v timed out dumped the message to file %v \n", clientID, clientID+".ndjson")
								continue ClientListLoop
							}
						} else {
							savePublishMessageToFile(clientID, publishMessage)
							log.Printf("client %v is unavailable dumping message to file %v \n", clientID, clientID+".ndjson")
						}
						// continue from the outer loop to avoid sending the
						// same message multiple times
						continue ClientListLoop
					}
				}
			}

		case <-h.exitchan:
			for _, client := range h.clients {
				close(client.send)
			}

			return
		}
	}
}

func (h *Hub) DumpSubscriptionsToFile(path string) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("Error opening file at path %v, error: %v\n", path, err)
	}
	defer file.Close()

	e := json.NewEncoder(file)
	err = e.Encode(h.subscriptionsMap)
	if err != nil {
		log.Println("Error marshalling subscriptions", err)
		return
	}

	log.Printf("Dump to file: %v done\n", path)
}

func savePublishMessageToFile(clientId string, message PublishMessage) {
	path := path.Join(*pubsubDataDirPath, clientId+".ndjson")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("Error opening file at path %v, error: %v\n", path, err)
	}
	defer file.Close()

	e := json.NewEncoder(file)
	err = e.Encode(message)
	if err != nil {
		log.Println("Error encoding publish message", err)
		return
	}
}

func savePublishMessagesToFile(clientId string, messages []PublishMessage) {
	path := path.Join(*pubsubDataDirPath, clientId+".ndjson")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("Error opening file at path %v, error: %v\n", path, err)
	}
	defer file.Close()

	e := json.NewEncoder(file)
	for _, message := range messages {
		err = e.Encode(message)
		if err != nil {
			log.Println("Error encoding publish message for client", clientId, err)
			continue
		}
	}
}

func readPendingPublishMessages(clientId string) ([]PublishMessage, error) {
	path := path.Join(*pubsubDataDirPath, clientId+".ndjson")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Printf("Error opening file at path %v, error: %v\n", path, err)
	}
	defer file.Close()

	var messages []PublishMessage
	d := json.NewDecoder(file)

	for {
		var message PublishMessage
		if err := d.Decode(&message); err == io.EOF {
			break // done decoding file
		} else if err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}

	err = file.Truncate(0)
	if err != nil {
		return nil, err
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	return messages, nil
}
