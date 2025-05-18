package event

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/The-Pocket/PocketFlow/go/logger"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

type SQLiteStateStore struct {
	db  *sql.DB
	log zerolog.Logger
}

func NewSQLiteStateStore(dbPath string) (*SQLiteStateStore, error) {
	log := logger.For("sqlite_store")
	log.Debug().Str("dbPath", dbPath).Msg("Opening SQLite database")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Error().Err(err).Str("dbPath", dbPath).Msg("Failed to open SQLite database")
		return nil, err
	}

	// Create tables if they don't exist
	log.Debug().Msg("Creating tables if they don't exist")
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS shared_data (
			flow_execution_id TEXT PRIMARY KEY,
			data TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS node_results (
			result_ref TEXT PRIMARY KEY,
			node_execution_id TEXT NOT NULL,
			step_type TEXT NOT NULL,
			result TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS flow_definitions (
			flow_id TEXT PRIMARY KEY,
			definition TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS flow_executions (
			flow_execution_id TEXT PRIMARY KEY,
			flow_definition_id TEXT NOT NULL
		);
	`)

	if err != nil {
		log.Error().Err(err).Msg("Failed to create database tables")
		db.Close()
		return nil, err
	}

	log.Info().Msg("SQLite state store initialized successfully")
	return &SQLiteStateStore{db: db, log: log}, nil
}

func (s *SQLiteStateStore) StoreSharedData(flowExecutionID string, data map[string]interface{}) error {
	s.log.Debug().Str("method", "StoreSharedData").Str("flowExecutionID", flowExecutionID).Msg("Entering StoreSharedData")
	s.log.Trace().Interface("data", data).Msg("Input data for StoreSharedData")

	jsonData, err := json.Marshal(data)
	if err != nil {
		s.log.Error().Err(err).Str("flowExecutionID", flowExecutionID).Msg("Failed to marshal shared data")
		return err
	}

	s.log.Debug().Str("sql", "INSERT OR REPLACE INTO shared_data (flow_execution_id, data) VALUES (?, ?)").Str("flowExecutionID", flowExecutionID).Msg("Executing SQL query")
	_, err = s.db.Exec(
		"INSERT OR REPLACE INTO shared_data (flow_execution_id, data) VALUES (?, ?)",
		flowExecutionID, string(jsonData),
	)

	if err != nil {
		s.log.Error().Err(err).Str("flowExecutionID", flowExecutionID).Msg("Failed to store shared data in database")
		return err
	}

	s.log.Trace().Str("flowExecutionID", flowExecutionID).Msg("Shared data stored successfully")
	s.log.Debug().Str("method", "StoreSharedData").Str("flowExecutionID", flowExecutionID).Msg("Exiting StoreSharedData")
	return nil
}

func (s *SQLiteStateStore) GetSharedData(flowExecutionID string) (map[string]interface{}, error) {
	s.log.Debug().Str("method", "GetSharedData").Str("flowExecutionID", flowExecutionID).Msg("Entering GetSharedData")
	var jsonData string
	s.log.Debug().Str("sql", "SELECT data FROM shared_data WHERE flow_execution_id = ?").Str("flowExecutionID", flowExecutionID).Msg("Executing SQL query")
	err := s.db.QueryRow(
		"SELECT data FROM shared_data WHERE flow_execution_id = ?",
		flowExecutionID,
	).Scan(&jsonData)

	if err != nil {
		if err == sql.ErrNoRows {
			s.log.Info().Str("flowExecutionID", flowExecutionID).Msg("No shared data found, returning empty map")
			return make(map[string]interface{}), nil
		}
		s.log.Error().Err(err).Str("flowExecutionID", flowExecutionID).Msg("Failed to get shared data from database")
		return nil, err
	}

	var data map[string]interface{}
	err = json.Unmarshal([]byte(jsonData), &data)
	if err != nil {
		s.log.Error().Err(err).Str("flowExecutionID", flowExecutionID).Msg("Failed to unmarshal shared data JSON")
		return nil, err
	}

	s.log.Trace().Str("flowExecutionID", flowExecutionID).Interface("data", data).Msg("Fetched shared data successfully")
	s.log.Debug().Str("method", "GetSharedData").Str("flowExecutionID", flowExecutionID).Msg("Exiting GetSharedData")
	return data, nil
}

func (s *SQLiteStateStore) StoreNodeResult(nodeExecutionID string, stepType string, result interface{}) (string, error) {
	s.log.Debug().Str("method", "StoreNodeResult").Str("nodeExecutionID", nodeExecutionID).Str("stepType", stepType).Msg("Entering StoreNodeResult")
	s.log.Trace().Interface("result", result).Msg("Input result for StoreNodeResult")
	jsonResult, err := json.Marshal(result)
	if err != nil {
		s.log.Error().Err(err).Str("nodeExecutionID", nodeExecutionID).Str("stepType", stepType).Msg("Failed to marshal node result")
		return "", err
	}

	resultRef := fmt.Sprintf("%s-%s", nodeExecutionID, stepType)
	s.log.Debug().Str("sql", "INSERT OR REPLACE INTO node_results (result_ref, node_execution_id, step_type, result) VALUES (?, ?, ?, ?)").Str("resultRef", resultRef).Msg("Executing SQL query")
	_, err = s.db.Exec(
		"INSERT OR REPLACE INTO node_results (result_ref, node_execution_id, step_type, result) VALUES (?, ?, ?, ?)",
		resultRef, nodeExecutionID, stepType, string(jsonResult),
	)

	if err != nil {
		s.log.Error().Err(err).Str("resultRef", resultRef).Msg("Failed to store node result in database")
		return "", err
	}

	s.log.Trace().Str("resultRef", resultRef).Msg("Node result stored successfully")
	s.log.Debug().Str("method", "StoreNodeResult").Str("resultRef", resultRef).Msg("Exiting StoreNodeResult")
	return resultRef, nil
}

func (s *SQLiteStateStore) GetNodeResult(resultRef string) (interface{}, error) {
	s.log.Debug().Str("method", "GetNodeResult").Str("resultRef", resultRef).Msg("Entering GetNodeResult")
	var jsonResult string
	s.log.Debug().Str("sql", "SELECT result FROM node_results WHERE result_ref = ?").Str("resultRef", resultRef).Msg("Executing SQL query")
	err := s.db.QueryRow(
		"SELECT result FROM node_results WHERE result_ref = ?",
		resultRef,
	).Scan(&jsonResult)

	if err != nil {
		s.log.Error().Err(err).Str("resultRef", resultRef).Msg("Failed to get node result from database")
		return nil, err
	}

	var result interface{}
	err = json.Unmarshal([]byte(jsonResult), &result)
	if err != nil {
		s.log.Error().Err(err).Str("resultRef", resultRef).Msg("Failed to unmarshal node result JSON")
		return nil, err
	}

	s.log.Trace().Str("resultRef", resultRef).Interface("result", result).Msg("Fetched node result successfully")
	s.log.Debug().Str("method", "GetNodeResult").Str("resultRef", resultRef).Msg("Exiting GetNodeResult")
	return result, nil
}

func (s *SQLiteStateStore) StoreFlowDefinition(flowID string, definition Flow) error {
	// Convert the Flow to JSON
	jsonData, err := json.Marshal(definition)
	if err != nil {
		return fmt.Errorf("failed to marshal flow definition: %w", err)
	}

	stmt, err := s.db.Prepare(`
		INSERT INTO flow_definitions (flow_id, definition)
		VALUES (?, ?)
		ON CONFLICT(flow_id) DO UPDATE SET definition = excluded.definition
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(flowID, jsonData)
	if err != nil {
		return fmt.Errorf("failed to store flow definition: %w", err)
	}

	return nil
}

func (s *SQLiteStateStore) GetFlowDefinition(flowID string) (Flow, error) {
	// This is a simplification. In reality, you'd need to
	// deserialize to the correct concrete type based on some metadata

	stmt, err := s.db.Prepare(`
		SELECT definition FROM flow_definitions
		WHERE flow_id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	var jsonData []byte
	err = stmt.QueryRow(flowID).Scan(&jsonData)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("flow definition not found: %s", flowID)
		}
		return nil, fmt.Errorf("failed to query flow definition: %w", err)
	}

	// Deserialize to flowDefinition
	var flow flowDefinition
	err = json.Unmarshal(jsonData, &flow)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal flow definition: %w", err)
	}

	return &flow, nil
}

func (s *SQLiteStateStore) GetFlowDefinitionByExecutionID(executionID string) (Flow, error) {
	// Get the flow ID for this execution
	flowID, err := s.getFlowIDForExecution(executionID)
	if err != nil {
		return nil, err
	}

	// Get the flow definition
	return s.GetFlowDefinition(flowID)
}

// Store a mapping between execution ID and definition ID
func (s *SQLiteStateStore) StoreFlowExecution(executionID string, definitionID string) error {
	s.log.Debug().Str("method", "StoreFlowExecution").Str("executionID", executionID).Str("definitionID", definitionID).Msg("Entering StoreFlowExecution")
	s.log.Debug().Str("sql", "INSERT INTO flow_executions (flow_execution_id, flow_definition_id) VALUES (?, ?)").Str("executionID", executionID).Str("definitionID", definitionID).Msg("Executing SQL query")
	_, err := s.db.Exec(
		"INSERT INTO flow_executions (flow_execution_id, flow_definition_id) VALUES (?, ?)",
		executionID, definitionID,
	)

	if err != nil {
		s.log.Error().Err(err).Str("executionID", executionID).Str("definitionID", definitionID).Msg("Failed to store flow execution in database")
		return err
	}

	s.log.Trace().Str("executionID", executionID).Str("definitionID", definitionID).Msg("Flow execution stored successfully")
	s.log.Debug().Str("method", "StoreFlowExecution").Str("executionID", executionID).Str("definitionID", definitionID).Msg("Exiting StoreFlowExecution")
	return err
}

func (s *SQLiteStateStore) UpdateSharedData(flowExecutionID string, nodeID string, result interface{}) error {
	// First get the existing shared data
	data, err := s.GetSharedData(flowExecutionID)
	if err != nil {
		return err
	}

	// Update the data with the result
	// Using the nodeID as the key for simplicity, but in a real implementation
	// you might want to use a more structured approach
	if result != nil {
		data[nodeID] = result
	}

	// Store the updated data
	return s.StoreSharedData(flowExecutionID, data)
}

// Helper function to get the flow ID for a given execution ID
func (s *SQLiteStateStore) getFlowIDForExecution(executionID string) (string, error) {
	stmt, err := s.db.Prepare(`
		SELECT flow_definition_id FROM flow_executions
		WHERE flow_execution_id = ?
	`)
	if err != nil {
		return "", fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	var flowID string
	err = stmt.QueryRow(executionID).Scan(&flowID)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("no flow ID found for execution ID: %s", executionID)
		}
		return "", fmt.Errorf("failed to query flow ID: %w", err)
	}

	return flowID, nil
}
