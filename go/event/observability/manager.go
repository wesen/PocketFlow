package observability

import (
	"fmt"
	"sync"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/rs/zerolog/log"
)

// DefaultObservabilityManager implements ObservabilityManager
type DefaultObservabilityManager struct {
	observers map[string]Observer
	mutex     sync.RWMutex
}

// NewObservabilityManager creates a new observability manager
func NewObservabilityManager() *DefaultObservabilityManager {
	return &DefaultObservabilityManager{
		observers: make(map[string]Observer),
	}
}

// AddObserver adds an observer to the manager
func (m *DefaultObservabilityManager) AddObserver(observer Observer) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	name := observer.GetName()
	if _, exists := m.observers[name]; exists {
		return fmt.Errorf("observer with name '%s' already exists", name)
	}

	m.observers[name] = observer
	log.Debug().Str("observer", name).Msg("Added observer")
	return nil
}

// RemoveObserver removes an observer from the manager
func (m *DefaultObservabilityManager) RemoveObserver(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.observers[name]; !exists {
		return fmt.Errorf("observer with name '%s' not found", name)
	}

	delete(m.observers, name)
	log.Debug().Str("observer", name).Msg("Removed observer")
	return nil
}

// NotifyObservers sends an event to all enabled observers
func (m *DefaultObservabilityManager) NotifyObservers(event ObservableEvent) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	var errs []error
	for name, observer := range m.observers {
		if observer.IsEnabled() {
			if err := observer.Observe(event); err != nil {
				log.Error().Err(err).Str("observer", name).Msg("Observer failed to process event")
				errs = append(errs, fmt.Errorf("observer '%s': %w", name, err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("one or more observers failed: %v", errs)
	}

	return nil
}

// GetObserver returns an observer by name
func (m *DefaultObservabilityManager) GetObserver(name string) Observer {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.observers[name]
}

// ListObservers returns all observers
func (m *DefaultObservabilityManager) ListObservers() []Observer {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	observers := make([]Observer, 0, len(m.observers))
	for _, observer := range m.observers {
		observers = append(observers, observer)
	}
	return observers
}

// EnableObserver enables an observer by name
func (m *DefaultObservabilityManager) EnableObserver(name string) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	observer, exists := m.observers[name]
	if !exists {
		return fmt.Errorf("observer with name '%s' not found", name)
	}

	observer.SetEnabled(true)
	log.Debug().Str("observer", name).Msg("Enabled observer")
	return nil
}

// DisableObserver disables an observer by name
func (m *DefaultObservabilityManager) DisableObserver(name string) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	observer, exists := m.observers[name]
	if !exists {
		return fmt.Errorf("observer with name '%s' not found", name)
	}

	observer.SetEnabled(false)
	log.Debug().Str("observer", name).Msg("Disabled observer")
	return nil
}

// CreateEventFromMessage creates an observable event from a PocketFlow message
func (m *DefaultObservabilityManager) CreateEventFromMessage(msg interface{}) (ObservableEvent, error) {
	switch message := msg.(type) {
	case *core.FlowStartRequestedMessage:
		return CreateFlowStartedEvent(message), nil
	case *core.FlowCompletedMessage:
		return CreateFlowCompletedEvent(message), nil
	case *core.FlowFailedMessage:
		return CreateFlowFailedEvent(message), nil
	case *core.ExecRequestedMessage:
		return CreateNodeStartedEvent(message), nil
	case *core.NodeCompletedMessage:
		return CreateNodeCompletedEvent(message), nil
	case *core.ExecFailedMessage:
		return CreateNodeFailedEvent(message), nil
	case *core.ProgressUpdateMessage:
		return CreateProgressUpdateEvent(message), nil
	default:
		return nil, fmt.Errorf("unsupported message type: %T", msg)
	}
}

// Ensure DefaultObservabilityManager implements ObservabilityManager interface
var _ ObservabilityManager = (*DefaultObservabilityManager)(nil)

// ObservableEventPublisher wraps an EventPublisher to emit observable events
type ObservableEventPublisher struct {
	publisher core.EventPublisher
	manager   ObservabilityManager
}

// NewObservableEventPublisher creates a new observable event publisher
func NewObservableEventPublisher(publisher core.EventPublisher, manager ObservabilityManager) *ObservableEventPublisher {
	return &ObservableEventPublisher{
		publisher: publisher,
		manager:   manager,
	}
}

// Publish publishes an event through the wrapped publisher
func (p *ObservableEventPublisher) Publish(topic string, event interface{}) error {
	return p.publisher.Publish(topic, event)
}

// PublishWithObservability publishes an event and notifies observers
func (p *ObservableEventPublisher) PublishWithObservability(topic string, event interface{}) error {
	// First publish the event normally
	err := p.publisher.Publish(topic, event)
	if err != nil {
		return err
	}

	// Create observable event and notify observers
	observableEvent, err := p.manager.CreateEventFromMessage(event)
	if err != nil {
		log.Debug().Err(err).Msg("Could not create observable event, skipping observation")
		return nil // Don't fail the publish operation
	}

	// Notify observers (don't fail publish if observers fail)
	if err := p.manager.NotifyObservers(observableEvent); err != nil {
		log.Error().Err(err).Msg("Failed to notify observers")
	}

	return nil
}

// AddObserver adds an observer to the manager
func (p *ObservableEventPublisher) AddObserver(observer Observer) error {
	return p.manager.AddObserver(observer)
}

// RemoveObserver removes an observer from the manager
func (p *ObservableEventPublisher) RemoveObserver(name string) error {
	return p.manager.RemoveObserver(name)
}

// NotifyObservers sends an event to all enabled observers
func (p *ObservableEventPublisher) NotifyObservers(event ObservableEvent) error {
	return p.manager.NotifyObservers(event)
}

// GetObserver returns an observer by name
func (p *ObservableEventPublisher) GetObserver(name string) Observer {
	return p.manager.GetObserver(name)
}

// ListObservers returns all observers
func (p *ObservableEventPublisher) ListObservers() []Observer {
	return p.manager.ListObservers()
}

// EnableObserver enables an observer by name
func (p *ObservableEventPublisher) EnableObserver(name string) error {
	return p.manager.EnableObserver(name)
}

// DisableObserver disables an observer by name
func (p *ObservableEventPublisher) DisableObserver(name string) error {
	return p.manager.DisableObserver(name)
}

// CreateEventFromMessage creates an observable event from a PocketFlow message
func (p *ObservableEventPublisher) CreateEventFromMessage(msg interface{}) (ObservableEvent, error) {
	return p.manager.CreateEventFromMessage(msg)
}

// Ensure ObservableEventPublisher implements EventPublisherObserver interface
var _ EventPublisherObserver = (*ObservableEventPublisher)(nil)