# PocketFlow Go Cookbook

This cookbook collects example flows implemented with the Go version of PocketFlow.
Each example demonstrates a particular feature or pattern.

| Example | Description |
|---------|-------------|
| [QA](./qa) | Basic question answering flow using the event driven system |
| [Branching](./branching) | Intent classifier with multiple response paths |
| [Memory](./memory) | Chat application that keeps short-term context |
| [Guardrail](./guardrail) | Travel chat with simple validation |

Run examples from the `go` directory. For instance:

```bash
# Run the QA flow
go run main.go --flow=qa

# Run the branching flow
go run main.go --flow=branching

# Run the memory flow
go run main.go --flow=memory

# Run the guardrail flow
go run main.go --flow=guardrail
```
