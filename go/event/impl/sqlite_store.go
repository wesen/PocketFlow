package impl

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// SQLiteStateStore uses SQLite for persistence
type SQLiteStateStore struct {
	db  *sql.DB
	log zerolog.Logger
}

// NewSQLiteStateStore creates a new SQLite-based state store
func NewSQLiteStateStore(dbPath string) (*SQLiteStateStore, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	store := &SQLiteStateStore{
		db:  db,
		log: log.With().Str("component", "SQLiteStateStore").Logger(),
	}

	// Initialize schema
	if err := store.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return store, nil
}

// initSchema initializes the database schema
func (s *SQLiteStateStore) initSchema() error {
	// Create shared data table
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS shared_data (
			flow_execution_id TEXT PRIMARY KEY,
			data TEXT NOT NULL
		)
	`)
	if err != nil {
		return err
	}

	// Create node results table
	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS node_results (
			result_id TEXT PRIMARY KEY,
			node_execution_id TEXT NOT NULL,
			step_type TEXT NOT NULL,
			result TEXT NOT NULL
		)
	`)
	if err != nil {
		return err
	}

	// Create flow definitions table
	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS flow_definitions (
			flow_id TEXT PRIMARY KEY,
			definition TEXT NOT NULL
		)
	`)
	if err != nil {
		return err
	}

	// Create flow executions table
	_, err = s.db.Exec(`
		CREATE TABLE IF NOT EXISTS flow_executions (
			execution_id TEXT PRIMARY KEY,
			definition_id TEXT NOT NULL
		)
	`)
	return err
}

// StoreSharedData stores shared data for a flow execution
func (s *SQLiteStateStore) StoreSharedData(flowExecutionID string, data map[string]interface{}) error {
	// Serialize data to JSON
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	// Insert or replace in the shared_data table
	_, err = s.db.Exec(
		"INSERT OR REPLACE INTO shared_data (flow_execution_id, data) VALUES (?, ?)",
		flowExecutionID, string(dataJSON),
	)
	if err != nil {
		return fmt.Errorf("failed to store shared data: %w", err)
	}

	return nil
}

// GetSharedData gets shared data for a flow execution
func (s *SQLiteStateStore) GetSharedData(flowExecutionID string) (map[string]interface{}, error) {
	// Query the shared_data table
	var dataJSON string
	err := s.db.QueryRow(
		"SELECT data FROM shared_data WHERE flow_execution_id = ?",
		flowExecutionID,
	).Scan(&dataJSON)

	if err != nil {
		if err == sql.ErrNoRows {
			return make(map[string]interface{}), nil // Return empty map if not found
		}
		return nil, fmt.Errorf("failed to get shared data: %w", err)
	}

	// Deserialize from JSON
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	return data, nil
}

// StoreNodeResult stores the result of a node's step
func (s *SQLiteStateStore) StoreNodeResult(nodeExecutionID string, stepType string, result interface{}) (string, error) {
	// Generate a unique result ID
	resultID := uuid.New().String()

	// Serialize result to JSON
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("failed to marshal result: %w", err)
	}

	// Insert into the node_results table
	_, err = s.db.Exec(
		"INSERT INTO node_results (result_id, node_execution_id, step_type, result) VALUES (?, ?, ?, ?)",
		resultID, nodeExecutionID, stepType, string(resultJSON),
	)
	if err != nil {
		return "", fmt.Errorf("failed to store node result: %w", err)
	}

	return resultID, nil
}

// GetNodeResult gets a stored result by reference
func (s *SQLiteStateStore) GetNodeResult(resultRef string) (interface{}, error) {
	// Query the node_results table
	var resultJSON string
	err := s.db.QueryRow(
		"SELECT result FROM node_results WHERE result_id = ?",
		resultRef,
	).Scan(&resultJSON)

	if err != nil {
		return nil, fmt.Errorf("failed to get node result: %w", err)
	}

	// Deserialize from JSON
	var result interface{}
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal result: %w", err)
	}

	return result, nil
}

// StoreFlowDefinition stores a flow definition
func (s *SQLiteStateStore) StoreFlowDefinition(flowID string, definition core.Flow) error {
	// NYI: Would need to serialize the flow definition
	return fmt.Errorf("not yet implemented")
}

// GetFlowDefinition gets a flow definition
func (s *SQLiteStateStore) GetFlowDefinition(flowID string) (core.Flow, error) {
	// NYI: Would need to deserialize the flow definition
	return nil, fmt.Errorf("not yet implemented")
}

// GetFlowDefinitionByExecutionID gets a flow definition by execution ID
func (s *SQLiteStateStore) GetFlowDefinitionByExecutionID(executionID string) (core.Flow, error) {
	// NYI: Would need to look up the flow ID and then get the definition
	return nil, fmt.Errorf("not yet implemented")
}

// StoreFlowExecution stores a mapping between execution ID and definition ID
func (s *SQLiteStateStore) StoreFlowExecution(executionID string, definitionID string) error {
	// Insert into the flow_executions table
	_, err := s.db.Exec(
		"INSERT INTO flow_executions (execution_id, definition_id) VALUES (?, ?)",
		executionID, definitionID,
	)
	if err != nil {
		return fmt.Errorf("failed to store flow execution: %w", err)
	}

	return nil
}

// UpdateSharedData updates shared data with node result
func (s *SQLiteStateStore) UpdateSharedData(flowExecutionID string, nodeID string, result interface{}) error {
	// Get current shared data
	sharedData, err := s.GetSharedData(flowExecutionID)
	if err != nil {
		return err
	}

	// Update with the node result
	sharedData[nodeID] = result

	// Store the updated shared data
	return s.StoreSharedData(flowExecutionID, sharedData)
}

// Close closes the database connection
func (s *SQLiteStateStore) Close() error {
	return s.db.Close()
}