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

func (s *SQLiteStateStore) StoreFlowDefinition(flowID string, definition *FlowDefinition) error {
	s.log.Debug().Str("method", "StoreFlowDefinition").Str("flowID", flowID).Msg("Entering StoreFlowDefinition")
	s.log.Trace().Interface("definition", definition).Msg("Input definition for StoreFlowDefinition")
	jsonDef, err := json.Marshal(definition)
	if err != nil {
		s.log.Error().Err(err).Str("flowID", flowID).Msg("Failed to marshal flow definition")
		return err
	}

	s.log.Debug().Str("sql", "INSERT OR REPLACE INTO flow_definitions (flow_id, definition) VALUES (?, ?)").Str("flowID", flowID).Msg("Executing SQL query")
	_, err = s.db.Exec(
		"INSERT OR REPLACE INTO flow_definitions (flow_id, definition) VALUES (?, ?)",
		flowID, string(jsonDef),
	)

	if err != nil {
		s.log.Error().Err(err).Str("flowID", flowID).Msg("Failed to store flow definition in database")
		return err
	}

	s.log.Trace().Str("flowID", flowID).Msg("Flow definition stored successfully")
	s.log.Debug().Str("method", "StoreFlowDefinition").Str("flowID", flowID).Msg("Exiting StoreFlowDefinition")
	return nil
}

func (s *SQLiteStateStore) GetFlowDefinition(flowID string) (*FlowDefinition, error) {
	s.log.Debug().Str("method", "GetFlowDefinition").Str("flowID", flowID).Msg("Entering GetFlowDefinition")
	var jsonDef string
	s.log.Debug().Str("sql", "SELECT definition FROM flow_definitions WHERE flow_id = ?").Str("flowID", flowID).Msg("Executing SQL query")
	err := s.db.QueryRow(
		"SELECT definition FROM flow_definitions WHERE flow_id = ?",
		flowID,
	).Scan(&jsonDef)

	if err != nil {
		s.log.Error().Err(err).Str("flowID", flowID).Msg("Failed to get flow definition from database")
		return nil, err
	}

	var definition FlowDefinition
	err = json.Unmarshal([]byte(jsonDef), &definition)
	if err != nil {
		s.log.Error().Err(err).Str("flowID", flowID).Msg("Failed to unmarshal flow definition JSON")
		return nil, err
	}

	s.log.Trace().Str("flowID", flowID).Interface("definition", definition).Msg("Fetched flow definition successfully")
	s.log.Debug().Str("method", "GetFlowDefinition").Str("flowID", flowID).Msg("Exiting GetFlowDefinition")
	return &definition, nil
}

func (s *SQLiteStateStore) GetFlowDefinitionByExecutionID(executionID string) (*FlowDefinition, error) {
	s.log.Debug().Str("method", "GetFlowDefinitionByExecutionID").Str("executionID", executionID).Msg("Entering GetFlowDefinitionByExecutionID")
	var flowDefID string
	s.log.Debug().Str("sql", "SELECT flow_definition_id FROM flow_executions WHERE flow_execution_id = ?").Str("executionID", executionID).Msg("Executing SQL query")
	err := s.db.QueryRow(
		"SELECT flow_definition_id FROM flow_executions WHERE flow_execution_id = ?",
		executionID,
	).Scan(&flowDefID)

	if err != nil {
		s.log.Error().Err(err).Str("executionID", executionID).Msg("Failed to get flow definition ID from flow_executions table")
		return nil, err
	}

	s.log.Trace().Str("executionID", executionID).Str("flowDefID", flowDefID).Msg("Fetched flow definition ID successfully")
	def, err := s.GetFlowDefinition(flowDefID)
	if err != nil {
		s.log.Error().Err(err).Str("flowDefID", flowDefID).Msg("Failed to get flow definition by ID")
		return nil, err
	}
	s.log.Debug().Str("method", "GetFlowDefinitionByExecutionID").Str("executionID", executionID).Msg("Exiting GetFlowDefinitionByExecutionID")
	return def, nil
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
