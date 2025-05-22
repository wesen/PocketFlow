package event

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/rs/zerolog/log"
)

// WatermillEventRouter uses Watermill for event routing
type WatermillEventRouter struct {
	NodeWorkers      map[string]core.NodeWorker
	FlowWorkers      map[string]core.FlowWorker
	PubSub           *gochannel.GoChannel
	Router           *message.Router
	Logger           watermill.LoggerAdapter
}

// WatermillPublisher implements the EventPublisher interface using Watermill
type WatermillPublisher struct {
	pubSub *gochannel.GoChannel
}

// NewWatermillPublisher creates a new publisher using Watermill
func NewWatermillPublisher(pubSub *gochannel.GoChannel) *WatermillPublisher {
	return &WatermillPublisher{pubSub: pubSub}
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

	err = p.pubSub.Publish(topic, msg)
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

	return &WatermillEventRouter{
		PubSub:      pubSub,
		Router:      router,
		NodeWorkers: make(map[string]core.NodeWorker),
		FlowWorkers: make(map[string]core.FlowWorker),
		Logger:      logger,
	}
}

// No longer need the UpdateOrchestrator method since we removed the orchestrator

// Start starts the router
func (r *WatermillEventRouter) Start(ctx context.Context) error {
	return r.Router.Run(ctx)
}

// Stop stops the router
func (r *WatermillEventRouter) Stop() error {
	return r.Router.Close()
}

// RegisterNodeWorker registers a node worker with the router
func (r *WatermillEventRouter) RegisterNodeWorker(worker core.NodeWorker) {
	nodeType := worker.NodeType()
	r.NodeWorkers[nodeType] = worker

	// Subscribe to the node's topic
	topic := fmt.Sprintf("node.%s", nodeType)
	handlerName := fmt.Sprintf("handle_%s_node", nodeType)
	
	r.Router.AddHandler(
		handlerName,
		topic,
		r.PubSub,
		"node.responses", // Unused but required by Watermill
		r.PubSub,
		func(msg *message.Message) ([]*message.Message, error) {
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
				return nil, err
			}
			
			log.Debug().
				Str("handlerName", handlerName).
				Str("topic", topic).
				Str("nodeType", nodeType).
				Str("messageID", msg.UUID).
				Msg("Router successfully handled node message")
				
			return nil, nil
		},
	)

	log.Info().
		Str("nodeType", nodeType).
		Str("topic", topic).
		Str("handlerName", handlerName).
		Msg("Registered node worker")
}

// RegisterAllNodeWorkers registers multiple node workers
func (r *WatermillEventRouter) RegisterAllNodeWorkers(workers ...core.NodeWorker) {
	for _, worker := range workers {
		r.RegisterNodeWorker(worker)
	}
}

// RegisterFlowWorker registers a flow worker with the router
func (r *WatermillEventRouter) RegisterFlowWorker(worker core.FlowWorker) {
	flowType := worker.FlowType()
	r.FlowWorkers[flowType] = worker

	// Subscribe to the flow's topic
	flowTopic := fmt.Sprintf("flow.%s", flowType)
	flowHandlerName := fmt.Sprintf("handle_%s_flow", flowType)
	
	r.Router.AddHandler(
		flowHandlerName,
		flowTopic,
		r.PubSub,
		"flow.responses", // Unused but required by Watermill
		r.PubSub,
		func(msg *message.Message) ([]*message.Message, error) {
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
				return nil, err
			}
			
			log.Debug().
				Str("handlerName", flowHandlerName).
				Str("topic", flowTopic).
				Str("flowType", flowType).
				Str("messageID", msg.UUID).
				Msg("Router successfully handled flow message")
				
			return nil, nil
		},
	)

	// Also subscribe the flow worker to node.completed events
	nodeCompletedHandlerName := fmt.Sprintf("handle_%s_node_completed", flowType)
	nodeCompletedTopic := "node.completed"
	
	r.Router.AddHandler(
		nodeCompletedHandlerName,
		nodeCompletedTopic,
		r.PubSub,
		"node.completed.responses", // Unused but required by Watermill
		r.PubSub,
		func(msg *message.Message) ([]*message.Message, error) {
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
				return nil, err
			}
			
			log.Debug().
				Str("handlerName", nodeCompletedHandlerName).
				Str("topic", nodeCompletedTopic).
				Str("flowType", flowType).
				Str("messageID", msg.UUID).
				Msg("Router successfully handled node completion message")
				
			return nil, nil
		},
	)

	log.Info().
		Str("flowType", flowType).
		Str("flowTopic", flowTopic).
		Str("flowHandlerName", flowHandlerName).
		Str("nodeCompletedHandlerName", nodeCompletedHandlerName).
		Msg("Registered flow worker")
}

// SetupFlowCompletionHandler sets up a handler for flow completed events
func (r *WatermillEventRouter) SetupFlowCompletionHandler(handler func(core.FlowCompletedMessage) error) {
	r.Router.AddHandler(
		"handle_flow_completed",
		"flow.completed",
		r.PubSub,
		"flow.completion.responses", // Unused but required by Watermill
		r.PubSub,
		func(msg *message.Message) ([]*message.Message, error) {
			var completed core.FlowCompletedMessage
			if err := json.Unmarshal(msg.Payload, &completed); err != nil {
				return nil, err
			}

			if err := handler(completed); err != nil {
				log.Error().Err(err).Msg("Error handling flow completion")
				return nil, err
			}
			return nil, nil
		},
	)
}

// SetupFlowFailureHandler sets up a handler for flow failed events
func (r *WatermillEventRouter) SetupFlowFailureHandler(handler func(core.FlowFailedMessage) error) {
	r.Router.AddHandler(
		"handle_flow_failed",
		"flow.failed",
		r.PubSub,
		"flow.failure.responses", // Unused but required by Watermill
		r.PubSub,
		func(msg *message.Message) ([]*message.Message, error) {
			var failed core.FlowFailedMessage
			if err := json.Unmarshal(msg.Payload, &failed); err != nil {
				return nil, err
			}

			if err := handler(failed); err != nil {
				log.Error().Err(err).Msg("Error handling flow failure")
				return nil, err
			}
			return nil, nil
		},
	)
}

// SetupProgressHandler sets up a handler for progress updates
func (r *WatermillEventRouter) SetupProgressHandler(handler func(core.ProgressUpdateMessage) error) {
	r.Router.AddHandler(
		"handle_progress",
		"progress",
		r.PubSub,
		"progress.responses", // Unused but required by Watermill
		r.PubSub,
		func(msg *message.Message) ([]*message.Message, error) {
			var progress core.ProgressUpdateMessage
			if err := json.Unmarshal(msg.Payload, &progress); err != nil {
				return nil, err
			}

			if err := handler(progress); err != nil {
				log.Error().Err(err).Msg("Error handling progress update")
				return nil, err
			}
			return nil, nil
		},
	)
}
