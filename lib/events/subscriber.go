package events

import (
        "context"
        "fmt"
	"log"
        "cloud.google.com/go/pubsub"
)

type subscriber struct {
	projectID string
	subscription string
	options map[string]string
	processEvent func(string, []byte) error
}

func NewSubscriber(projectID string, subscription string, options map[string]string, processEvent func(string, []byte) error) (*subscriber) {
	return &subscriber{projectID, subscription, options, processEvent}
}

func (s *subscriber) Consume() {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, s.projectID)

	if err != nil {
		// TODO: Handle error.
		log.Fatal("Failed to instantiate pubsub client ", err)
	}

	sub := client.Subscription(s.subscription)
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		err = s.processEvent(m.ID, m.Data)

		if err != nil {
			fmt.Println("Oh no")
			m.Nack()
		} else {
			m.Ack()
		}
	})

	if err != context.Canceled {
		// TODO: Handle error.
		fmt.Println("Eek")
	}	
}
