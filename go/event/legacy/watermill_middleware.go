package event

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
	
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/rs/zerolog/log"
)

// PoisonQueueMiddleware prevents endless retries of failing messages
func PoisonQueueMiddleware(maxRetries int) message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			// Check if the message has metadata for retry count
			retryCount := 0
			if retryCountStr := msg.Metadata.Get("retry_count"); retryCountStr != "" {
				if _, err := fmt.Sscanf(retryCountStr, "%d", &retryCount); err != nil {
					retryCount = 0
				}
			}

			// Extract message info for logging
			var base BaseMessage
			_ = json.Unmarshal(msg.Payload, &base) // Ignore error

			// Check if we've reached the max retries
			if retryCount >= maxRetries {
				log.Warn().
					Int("retryCount", retryCount).
					Str("messageID", msg.UUID).
					Str("messageType", base.MessageType).
					Str("flowExecutionID", base.FlowExecutionID).
					Msg("Message retries exceeded, discarding poisoned message")
				
				// Return without error to acknowledge and remove the message
				return nil, nil
			}

			// Call the original handler
			messages, err := h(msg)

			// If there was an error, increment the retry count
			if err != nil {
				log.Warn().
					Err(err).
					Int("retryCount", retryCount).
					Str("messageID", msg.UUID).
					Str("messageType", base.MessageType).
					Str("flowExecutionID", base.FlowExecutionID).
					Msg("Message handling failed, will retry")

				// Increment retry count in metadata
				for i := range messages {
					messages[i].Metadata.Set("retry_count", fmt.Sprintf("%d", retryCount+1))
				}
			}

			return messages, err
		}
	}
}

// LoggingMiddleware logs detailed information about message processing
func LoggingMiddleware() message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			start := time.Now()

			// Extract message info for logging
			var base BaseMessage
			_ = json.Unmarshal(msg.Payload, &base) // Ignore error

			// Log the incoming message
			l := log.Debug().  
				Str("messageID", msg.UUID).
				Str("messageType", base.MessageType).
				Str("flowExecutionID", base.FlowExecutionID)

			if base.NodeExecutionID != "" {
				l = l.Str("nodeExecutionID", base.NodeExecutionID)
			}

			l.Msg("Processing message")

			// Call the original handler
			messages, err := h(msg)

			// Log the result
			duration := time.Since(start)

			if err != nil {
				log.Error().
					Err(err).
					Dur("duration", duration).
					Str("messageID", msg.UUID).
					Str("messageType", base.MessageType).
					Msg("Message processing failed")
			} else {
				log.Debug().
					Int("producedMessages", len(messages)).
					Dur("duration", duration).
					Str("messageID", msg.UUID).
					Msg("Message processed successfully")
			}

			return messages, err
		}
	}
}

// RecoveryMiddleware prevents panics from crashing the application
func RecoveryMiddleware() message.HandlerMiddleware {
	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) (messages []*message.Message, err error) {
			defer func() {
				if r := recover(); r != nil {
					// Extract message info for logging
					var base BaseMessage
					_ = json.Unmarshal(msg.Payload, &base) // Ignore error

					log.Error().
						Interface("panic", r).
						Str("messageID", msg.UUID).
						Str("messageType", base.MessageType).
						Str("flowExecutionID", base.FlowExecutionID).
						Msg("Recovered from panic in message handler")

					// Return an error to trigger retry logic
					messages = nil
					err = fmt.Errorf("panic recovered: %v", r)
				}
			}()

			return h(msg)
		}
	}
}

// ThrottlingMiddleware limits the rate of message processing
func ThrottlingMiddleware(rate time.Duration) message.HandlerMiddleware {
	var lastProcessed time.Time
	var mutex = &sync.Mutex{}

	return func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			mutex.Lock()
			
			now := time.Now()
			timeSinceLast := now.Sub(lastProcessed)
			
			if timeSinceLast < rate && !lastProcessed.IsZero() {
				sleepTime := rate - timeSinceLast
				mutex.Unlock()
				time.Sleep(sleepTime)
			} else {
				mutex.Unlock()
			}
			
			mutex.Lock()
			lastProcessed = time.Now()
			mutex.Unlock()
			
			return h(msg)
		}
	}
}