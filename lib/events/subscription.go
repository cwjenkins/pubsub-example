package events

import (
        "context"
        "fmt"
        "time"

        "cloud.google.com/go/pubsub"
)

type subscription struct {
	projectID string
	topic string
	name string
}

func NewSubscription(projectID string, topic string, name string) (*subscription) {
	return &subscription{projectID, topic, name}
}

func (s *subscription) CreateIfMissing() error {
        // subID := "my-sub"
        // topicID := "my-topic"
        // fullyQualifiedDeadLetterTopic := "projects/my-project/topics/my-dead-letter-topic"
        ctx := context.Background()
        client, err := pubsub.NewClient(ctx, s.projectID)

        if err != nil {
                return fmt.Errorf("pubsub.NewClient: %v", err)
        }
        defer client.Close()

        topic := client.Topic(s.topic)

        subConfig := pubsub.SubscriptionConfig{
                Topic:       topic,
                AckDeadline: 20 * time.Second,
		//                DeadLetterPolicy: &pubsub.DeadLetterPolicy{
		//                        DeadLetterTopic:     fullyQualifiedDeadLetterTopic,
		//                        MaxDeliveryAttempts: 10,
		//                },
        }

        sub, err := client.CreateSubscription(ctx, s.name, subConfig)
        if err != nil {
                return fmt.Errorf("CreateSubscription: %v", err)
        }

	//	fmt.Printf("Created subscription (%s) with dead letter topic (%s)\n", sub.String(), fullyQualifiedDeadLetterTopic)
	fmt.Printf("Created subscription (%s)", sub.String())
        fmt.Printf("To process dead letter messages, remember to add a subscription to your dead letter topic.")
        return nil
}
