package event

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"github.com/sony/gobreaker"
	"time"
)

// WatermillEventRouter uses Watermill for event routing
type WatermillEventRouter struct {
	NodeWorkers      map[string]core.NodeWorker
	FlowWorkers      map[string]core.FlowWorker
	Publisher        message.Publisher
	Subscriber       message.Subscriber
	Router           *message.Router
	Logger           watermill.LoggerAdapter
}

// WatermillPublisher implements the EventPublisher interface using Watermill
type WatermillPublisher struct {
	publisher message.Publisher
}

var _ core.EventPublisher = (*WatermillPublisher)(nil)

// WatermillSubscriber implements the EventSubscriber interface using Watermill
// NewWatermillPublisher creates a new publisher using Watermill
func NewWatermillPublisher(publisher message.Publisher) *WatermillPublisher {
	return &WatermillPublisher{publisher: publisher}
}

// Publish publishes an event to a topic
func (p *WatermillPublisher) Publish(topic string, event interface{}) error {
	// Marshal the event to JSON
	payload, err := json.Marshal(event)
	if err != nil {
		log.Error().Err(err).Str("topic", topic).Msg("Publisher failed to marshal message")
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	messageUUID := watermill.NewUUID()
	msg := message.NewMessage(messageUUID, payload)
	
	// Extract message type for logging
	var messageType string
	if baseMsg, ok := event.(interface{ GetMessageType() string }); ok {
		messageType = baseMsg.GetMessageType()
	} else {
		// Try to extract from BaseMessage fields
		if jsonMap := make(map[string]interface{}); json.Unmarshal(payload, &jsonMap) == nil {
			if mt, exists := jsonMap["message_type"]; exists {
				messageType = fmt.Sprintf("%v", mt)
			}
		}
	}

	log.Debug().
		Str("topic", topic).
		Str("messageType", messageType).
		Str("messageID", messageUUID).
		Interface("event", event).
		Msg("Publisher publishing message")

	err = p.publisher.Publish(topic, msg)
	if err != nil {
		log.Error().
			Err(err).
			Str("topic", topic).
			Str("messageType", messageType).
			Str("messageID", messageUUID).
			Msg("Publisher failed to publish message")
		return err
	}
	
	log.Debug().
		Str("topic", topic).
		Str("messageType", messageType).
		Str("messageID", messageUUID).
		Msg("Publisher successfully published message")

	return nil
}

// setupRouterMiddlewares configures useful middlewares for the router
func SetupRouterMiddlewares(router *message.Router, deadLetterPublisher message.Publisher, logger watermill.LoggerAdapter) {
	// Circuit breaker middleware - prevents cascading failures
	circuitBreakerSettings := gobreaker.Settings{
		Name:        "pocketflow_circuit_breaker",
		MaxRequests: 10,
		Interval:    30 * time.Second,
		Timeout:     60 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.6
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Info().
				Str("circuit_breaker", name).
				Str("from_state", fmt.Sprintf("%v", from)).
				Str("to_state", fmt.Sprintf("%v", to)).
				Msg("Circuit breaker state changed")
		},
	}
	circuitBreakerMiddleware := middleware.NewCircuitBreaker(circuitBreakerSettings)

	// Retry middleware with exponential backoff
	retryMiddleware := middleware.Retry{
		MaxRetries:      3,
		InitialInterval: 100 * time.Millisecond,
		MaxInterval:     1 * time.Second,
		Multiplier:      2.0,
		Logger:          logger,
		OnRetryHook: func(retryNum int, delay time.Duration) {
			log.Warn().
				Int("retry_attempt", retryNum).
				Dur("delay", delay).
				Msg("🔄 Retrying message processing")
		},
	}

	// Timeout middleware - prevents hanging handlers
	timeoutMiddleware := middleware.Timeout(30 * time.Second)

	// Recovery middleware - prevents panics from crashing the router
	recoveryMiddleware := middleware.Recoverer

	// Poison queue middleware - moves permanently failing messages to dead letter queue
	var poisonQueueMiddleware message.HandlerMiddleware
	if deadLetterPublisher != nil {
		var err error
		log.Info().Msg("💀 Dead letter queue configured for permanently failed messages")
		poisonQueueMiddleware, err = middleware.PoisonQueue(deadLetterPublisher, "dead_letter_queue")
		if err != nil {
			log.Error().Err(err).Msg("Failed to create poison queue middleware")
		} else {
			log.Info().Msg("💀 Dead letter queue configured for permanently failed messages")
		}
	}

	// Install middlewares in order (they wrap handlers in reverse order)
	router.AddMiddleware(recoveryMiddleware)
	router.AddMiddleware(timeoutMiddleware)
	if poisonQueueMiddleware != nil {
		router.AddMiddleware(poisonQueueMiddleware)
	}
	router.AddMiddleware(retryMiddleware.Middleware)
	_ = circuitBreakerMiddleware
	// router.AddMiddleware(circuitBreakerMiddleware.Middleware)

	// Optional: Add correlation ID middleware for distributed tracing
	correlationMiddleware := middleware.CorrelationID
	router.AddMiddleware(correlationMiddleware)

	// Optional: Add instant ack for high throughput (if needed)
	// router.AddMiddleware(middleware.InstantAck)

	log.Info().Msg("✅ Router middlewares configured: recovery, timeout, poison_queue, retry, circuit_breaker, correlation_id")
}

// NewWatermillEventRouter creates a new router using Watermill
func NewWatermillEventRouter(logger watermill.LoggerAdapter) *WatermillEventRouter {
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			BlockPublishUntilSubscriberAck: false,
		},
		logger,
	)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create router")
	}

	// Setup middlewares for in-memory router (no dead letter queue for simplicity)
	SetupRouterMiddlewares(router, nil, logger)

	return &WatermillEventRouter{
		Publisher:   pubSub,
		Subscriber:  pubSub,
		Router:      router,
		NodeWorkers: make(map[string]core.NodeWorker),
		FlowWorkers: make(map[string]core.FlowWorker),
		Logger:      logger,
	}
}

// No longer need the UpdateOrchestrator method since we removed the orchestrator

// Start starts the router
func (r *WatermillEventRouter) Start(ctx context.Context) error {
	go func() {
		if err := r.Router.Run(ctx); err != nil {
			log.Error().Err(err).Msg("Router stopped with error")
		}
	}()
	return nil
}

// Stop stops the router
func (r *WatermillEventRouter) Stop() error {
	return r.Router.Close()
}

// RegisterNodeWorker registers a node worker with the router
func (r *WatermillEventRouter) RegisterNodeWorker(worker core.NodeWorker) error {
	if r.IsRunning() {
		return fmt.Errorf("cannot register node worker while router is running")
	}

	nodeType := worker.NodeType()
	r.NodeWorkers[nodeType] = worker

	// Subscribe to the node's topic
	topic := fmt.Sprintf("node.%s", nodeType)
	handlerName := fmt.Sprintf("handle_%s_node", nodeType)
	
	r.Router.AddNoPublisherHandler(
		handlerName,
		topic,
		r.Subscriber,
		func(msg *message.Message) error {
			log.Debug().
				Str("handlerName", handlerName).
				Str("topic", topic).
				Str("nodeType", nodeType).
				Str("messageID", msg.UUID).
				Msg("Router received message for node worker")
			
			if err := worker.HandleMessage(msg); err != nil {
				log.Error().
					Err(err).
					Str("handlerName", handlerName).
					Str("topic", topic).
					Str("nodeType", nodeType).
					Str("messageID", msg.UUID).
					Msg("Router error handling node message")
				return err
			}
			
			log.Debug().
				Str("handlerName", handlerName).
				Str("topic", topic).
				Str("nodeType", nodeType).
				Str("messageID", msg.UUID).
				Msg("Router successfully handled node message")
				
			return nil
		},
	)

	log.Info().
		Str("nodeType", nodeType).
		Str("topic", topic).
		Str("handlerName", handlerName).
		Msg("Registered node worker")
	
	return nil
}

// RegisterAllNodeWorkers registers multiple node workers
func (r *WatermillEventRouter) RegisterAllNodeWorkers(workers ...core.NodeWorker) error {
	for _, worker := range workers {
		if err := r.RegisterNodeWorker(worker); err != nil {
			return err
		}
	}
	return nil
}

// RegisterFlowWorker registers a flow worker with the router
func (r *WatermillEventRouter) RegisterFlowWorker(worker core.FlowWorker) error {
	if r.IsRunning() {
		return fmt.Errorf("cannot register flow worker while router is running")
	}

	flowType := worker.FlowType()
	r.FlowWorkers[flowType] = worker

	// Subscribe to the flow's topic
	flowTopic := fmt.Sprintf("flow.%s", flowType)
	flowHandlerName := fmt.Sprintf("handle_%s_flow", flowType)
	
	r.Router.AddNoPublisherHandler(
		flowHandlerName,
		flowTopic,
		r.Subscriber,
		func(msg *message.Message) error {
			log.Debug().
				Str("handlerName", flowHandlerName).
				Str("topic", flowTopic).
				Str("flowType", flowType).
				Str("messageID", msg.UUID).
				Msg("Router received message for flow worker")
			
			if err := worker.HandleMessage(msg); err != nil {
				log.Error().
					Err(err).
					Str("handlerName", flowHandlerName).
					Str("topic", flowTopic).
					Str("flowType", flowType).
					Str("messageID", msg.UUID).
					Msg("Router error handling flow message")
				return err
			}
			
			log.Debug().
				Str("handlerName", flowHandlerName).
				Str("topic", flowTopic).
				Str("flowType", flowType).
				Str("messageID", msg.UUID).
				Msg("Router successfully handled flow message")
				
			return nil
		},
	)

	// Also subscribe the flow worker to node.completed events
	nodeCompletedHandlerName := fmt.Sprintf("handle_%s_node_completed", flowType)
	nodeCompletedTopic := "node.completed"
	
	r.Router.AddNoPublisherHandler(
		nodeCompletedHandlerName,
		nodeCompletedTopic,
		r.Subscriber,
		func(msg *message.Message) error {
			log.Debug().
				Str("handlerName", nodeCompletedHandlerName).
				Str("topic", nodeCompletedTopic).
				Str("flowType", flowType).
				Str("messageID", msg.UUID).
				Msg("Router received node completion message for flow worker")
			
			if err := worker.HandleNodeCompletedMessage(msg); err != nil {
				log.Error().
					Err(err).
					Str("handlerName", nodeCompletedHandlerName).
					Str("topic", nodeCompletedTopic).
					Str("flowType", flowType).
					Str("messageID", msg.UUID).
					Msg("Router error handling node completion")
				return err
			}
			
			log.Debug().
				Str("handlerName", nodeCompletedHandlerName).
				Str("topic", nodeCompletedTopic).
				Str("flowType", flowType).
				Str("messageID", msg.UUID).
				Msg("Router successfully handled node completion message")
				
			return nil
		},
	)

	log.Info().
		Str("flowType", flowType).
		Str("flowTopic", flowTopic).
		Str("flowHandlerName", flowHandlerName).
		Str("nodeCompletedHandlerName", nodeCompletedHandlerName).
		Msg("Registered flow worker")
	
	return nil
}

// SetupFlowCompletionHandler sets up a handler for flow completed events
func (r *WatermillEventRouter) SetupFlowCompletionHandler(handler func(core.FlowCompletedMessage) error) {
	r.Router.AddNoPublisherHandler(
		"handle_flow_completed",
		"flow.completed",
		r.Subscriber,
		func(msg *message.Message) error {
			var completed core.FlowCompletedMessage
			if err := json.Unmarshal(msg.Payload, &completed); err != nil {
				return err
			}

			if err := handler(completed); err != nil {
				log.Error().Err(err).Msg("Error handling flow completion")
				return err
			}
			return nil
		},
	)
}

// IsRunning returns true if the router is running
func (r *WatermillEventRouter) IsRunning() bool {
	select {
	case <-r.Router.Running():
		return true
	default:
		return false
	}
}



// SetupFlowFailureHandler sets up a handler for flow failed events
func (r *WatermillEventRouter) SetupFlowFailureHandler(handler func(core.FlowFailedMessage) error) {
	r.Router.AddNoPublisherHandler(
		"handle_flow_failed",
		"flow.failed",
		r.Subscriber,
		func(msg *message.Message) error {
			var failed core.FlowFailedMessage
			if err := json.Unmarshal(msg.Payload, &failed); err != nil {
				return err
			}

			if err := handler(failed); err != nil {
				log.Error().Err(err).Msg("Error handling flow failure")
				return err
			}
			return nil
		},
	)
}

// SetupProgressHandler sets up a handler for progress updates
func (r *WatermillEventRouter) SetupProgressHandler(handler func(core.ProgressUpdateMessage) error) {
	r.Router.AddNoPublisherHandler(
		"handle_progress",
		"progress",
		r.Subscriber,
		func(msg *message.Message) error {
			var progress core.ProgressUpdateMessage
			if err := json.Unmarshal(msg.Payload, &progress); err != nil {
				return err
			}

			if err := handler(progress); err != nil {
				log.Error().Err(err).Msg("Error handling progress update")
				return err
			}
			return nil
		},
	)
}

// NewWatermillEventRouterWithRedis creates a new router using Watermill with Redis Streams
func NewWatermillEventRouterWithRedis(redisAddr string, logger watermill.LoggerAdapter) (*WatermillEventRouter, error) {
	if logger == nil {
		logger = watermill.NewStdLogger(false, false)
	}

	// Create Redis client
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0,
	})

	// Test Redis connection
	ctx := context.Background()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s: %w", redisAddr, err)
	}

	// Create Redis publisher
	publisher, err := redisstream.NewPublisher(
		redisstream.PublisherConfig{
			Client:     redisClient,
			Marshaller: redisstream.DefaultMarshallerUnmarshaller{},
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis publisher: %w", err)
	}

	// Create Redis subscriber for main application
	subscriber, err := redisstream.NewSubscriber(
		redisstream.SubscriberConfig{
			Client:        redisClient,
			Unmarshaller:  redisstream.DefaultMarshallerUnmarshaller{},
			ConsumerGroup: "pocketflow_main",
			Consumer:      "main_consumer",
		},
		logger,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis subscriber: %w", err)
	}

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create router: %w", err)
	}

	// Setup middlewares for Redis router with dead letter queue support
	SetupRouterMiddlewares(router, publisher, logger)

	return &WatermillEventRouter{
		Publisher:   publisher,
		Subscriber:  subscriber,
		Router:      router,
		NodeWorkers: make(map[string]core.NodeWorker),
		FlowWorkers: make(map[string]core.FlowWorker),
		Logger:      logger,
	}, nil
}
