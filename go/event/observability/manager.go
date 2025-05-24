package observability

import (
	"fmt"
	"sync"

	"github.com/The-Pocket/PocketFlow/go/event/event"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/rs/zerolog/log"
)

// DefaultObservabilityManager implements ObservabilityManager
type DefaultObservabilityManager struct {
	observers map[string]Observer
	router    *event.WatermillEventRouter
	mutex     sync.RWMutex
	started   bool
}

// NewObservabilityManager creates a new observability manager
func NewObservabilityManager(router *event.WatermillEventRouter) *DefaultObservabilityManager {
	m := &DefaultObservabilityManager{
		observers: make(map[string]Observer),
		router:    router,
	}
	router.Router.AddNoPublisherHandler(
		"observability-manager",
		"observability",
		router.Subscriber,
		func(msg *message.Message) error {
			m.mutex.RLock()
			for _, observer := range m.observers {
				if observer.IsEnabled() {
					if err := observer.HandleMessage(msg); err != nil {
						log.Error().Err(err).Str("observer", observer.GetName()).Msg("Observer failed to handle message")
					}
				}
			}
			m.mutex.RUnlock()
			return nil
		})
	return m
}

// AddObserver adds an observer and subscribes it to its topics
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

// Start begins the observability system
func (m *DefaultObservabilityManager) Start() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.started {
		return fmt.Errorf("observability manager already started")
	}

	m.started = true
	log.Info().Msg("Observability system started")
	return nil
}

// Stop shuts down the observability system
func (m *DefaultObservabilityManager) Stop() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if !m.started {
		return nil
	}

	// Note: We don't unsubscribe here because the EventSubscriber interface
	// doesn't provide an unsubscribe method. This is typically handled
	// by the underlying message system when it shuts down.

	m.started = false
	log.Info().Msg("Observability system stopped")
	return nil
}

// Ensure DefaultObservabilityManager implements ObservabilityManager interface
var _ ObservabilityManager = (*DefaultObservabilityManager)(nil)
