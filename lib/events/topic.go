package events

import (
        "context"
        "fmt"
        "log"

        "cloud.google.com/go/pubsub"
)

type topic struct {
	projectID string
	name string
}

func NewTopic(projectID string, name string) (*topic) {
	return &topic{projectID, name}
}

func (t *topic) CreateIfMissing() {
        ctx := context.Background()

        // Creates a client.
        client, err := pubsub.NewClient(ctx, t.projectID)
        if err != nil {
                log.Fatalf("Failed to create client: %v", err)
        }
        defer client.Close()

        // Creates the new topic.
        newTopic, err := client.CreateTopic(ctx, t.name)
        if err != nil {
                log.Fatalf("Failed to create topic: %v", err)
        }

        fmt.Printf("Topic %v created.\n", newTopic)
}
