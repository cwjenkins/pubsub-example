package main

import (
	"fmt"
	"colton.jenkins/lib/events"
)

func main() {
	// Will convert to env vars
	projectID := "PROJECT_ID"
	//	topic := "identity"
	subscription := "events"

	// One time setup (should live in service)
	//	setupTopic(projectID, topic)

	// One time setup
	//	setupSubscription(projectID, topic, subscription)

	// Place holder for now
	options := make(map[string]string)
	options["threads"] = "1"

  	sub := events.NewSubscriber(projectID, subscription, options, processEvent)

	// Blocking
	sub.Consume()
}

func setupTopic(projectID string, topic string) {
	t := events.NewTopic(projectID, topic)
	t.CreateIfMissing()
}

func setupSubscription(projectID string, topic string, subscription string) {
	s := events.NewSubscription(projectID, topic, subscription)
	s.CreateIfMissing()
}

func processEvent(id string, data []byte) error {
	fmt.Printf("Message ID: %s, Message Data: %s\n", id, string(data))
	return nil
}
