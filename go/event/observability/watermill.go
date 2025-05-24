package observability

import (
	"fmt"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/event"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/redis/go-redis/v9"
)

// NewObservabilityRouterWithRedis creates a separate router for observability with its own consumer group
func NewObservabilityRouterWithRedis(redisAddr string, logger watermill.LoggerAdapter) (*event.WatermillEventRouter, error) {
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})

	// Create Redis subscriber for observability with separate consumer group
	subscriber, err := redisstream.NewSubscriber(
		redisstream.SubscriberConfig{
			Client:        redisClient,
			Unmarshaller:  redisstream.DefaultMarshallerUnmarshaller{},
			ConsumerGroup: "pocketflow_observability",
			Consumer:      "observability_consumer",
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis subscriber for observability: %w", err)
	}

	publisher, err := redisstream.NewPublisher(
		redisstream.PublisherConfig{
			Client:     redisClient,
			Marshaller: redisstream.DefaultMarshallerUnmarshaller{},
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis publisher for observability: %w", err)
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create router for observability: %w", err)
	}

	// Add middleware to immediately acknowledge messages
	router.AddMiddleware(middleware.InstantAck)

	return &event.WatermillEventRouter{
		Publisher:   publisher,
		Subscriber:  subscriber,
		Router:      router,
		NodeWorkers: make(map[string]core.NodeWorker),
		FlowWorkers: make(map[string]core.FlowWorker),
		Logger:      logger,
	}, nil
}
