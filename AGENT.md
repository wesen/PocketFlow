# PocketFlow Agent Guidelines

## Build and Test Commands
- Install: `pip install -e .`
- Run a specific test: `python -m unittest tests/test_flow_basic.py`
- Run all tests: `python -m unittest discover tests`

## Code Style Guidelines
- Follow PEP 8 standards for Python code
- Use 4-space indentation
- Node implementation: Implement `prep()`, `exec()`, and `post()` methods
- Flow design: Use `>>` for default transitions and `-` for named transitions
- Prefer descriptive names for Nodes, Flows, and actions
- Return `None` from `post()` for default transitions
- Return action string from `post()` for conditional transitions
- Shared store: Use dictionary for data communication between nodes
- Keep each Node focused on a single responsibility
- Document complex logic with clear docstrings
- Use proper error handling with retry mechanisms when needed

<goGuidelines>
When implementing go interfaces, use the var _ Interface = &Foo{} to make sure the interface is always implemented correctly.
When building web applications, use htmx, bootstrap and the templ templating language.
Always use a context argument when appropriate.
Use cobra for command-line applications.
Use the "defaults" package name, instead of "default" package name, as it's reserved in go.
Use github.com/pkg/errors for wrapping errors.
When starting goroutines, use errgroup.
go doesn't support the ternary operator, use if else instead.
</goGuidelines>