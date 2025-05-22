# PocketFlow Go Implementation

This directory contains the Go implementation of PocketFlow, a lightweight yet powerful framework for building complex LLM-powered applications using a graph-based workflow approach.

## Architecture Overview

The Go implementation follows an event-driven architecture where:

- **Nodes** represent individual task processors (like questioning a user or calling an LLM)
- **Flows** connect nodes together in a flexible, declarative way
- **Workers** handle the execution of nodes and flows
- **Messages** communicate between components using a publish-subscribe pattern
- **Topics** route messages to the appropriate handlers

This architecture provides several key advantages:

- **Decoupling**: Business logic (flows) is separated from execution logic (workers)
- **Flexibility**: Flows can be defined, modified, and visualized declaratively
- **Extensibility**: New node and flow types can be added without changing the core system
- **Observability**: Progress updates and execution status are built into the framework
- **Scalability**: Components can be distributed and scaled independently

## Quick Start

### Running Example Flows

You can run one of the included example flows:

```bash
# Run the basic Q&A flow
go run main.go --flow=basic

# Run the question-answering flow
go run main.go --flow=qa

# Run the branching intent flow
go run main.go --flow=branching

# Just visualize a flow without running it
go run main.go --flow=branching --visualize
```

### Creating a Custom Flow

To create a custom flow, you'll need to:

1. Create node workers for each node type
2. Register the node workers with the runner
3. Define the nodes using the node workers' `NewNode()` method
4. Connect them using the `impl.NewFlowBuilder()` builder
5. Register the flow with the runner

Here's a simple example:

```go
// Create node workers first
greetingWorker := event.NewGreetingNodeWorker(publisher, stateStore)
farewellWorker := event.NewFarewellNodeWorker(publisher, stateStore)

// Register workers with the runner
runner.RegisterNodeWorkers(greetingWorker, farewellWorker)

// Define nodes using the node workers
greetingNode := greetingWorker.NewNode(event.NodeParams{
    "message": "Hello, world!",
})
farewellNode := farewellWorker.NewNode(event.NodeParams{
    "message": "Goodbye, world!",
})

// Define flow using builder pattern
simpleFlow := impl.NewFlowBuilder().
    Begin(greetingNode).
    Then(farewellNode).
    Build()
```

## Directory Structure

- `core/` - Core interfaces and message definitions
- `impl/` - Concrete implementations of the core interfaces
- `examples/` - Example flows demonstrating different patterns

## Creating Node Workers

Node workers are where the actual processing logic happens. You can implement a custom node worker by:

1. Implementing the `SimpleNodeHandler` interface
2. Providing `Prep`, `Exec`, and `Post` methods
3. Wrapping it in a `SimpleNode` for registration

For simple cases, you can also use the `NodeBuilder` with callback functions.

## Advanced Features

- **Branching Flows**: Create complex workflows with decision points
- **Progress Tracking**: Monitor the status of flow execution
- **Error Handling**: Implement retry and recovery mechanisms
- **Visualization**: Generate Mermaid diagrams of flow structures