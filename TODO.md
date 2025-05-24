- [ ] implement sqlite state store 

- [ ] Continue making sure that the observer is not a publisher proxy, but is just registering as subscriber
    - [ ] make sure that the actual implementation is not brain broken
    - [x] I think the observability system should run its own router/runner and each new flow run should create its own as well
    - [ ] RunHandlers doesn't need to run in its own subroutine and I don't think it's needed anyway (see preivous point)
    - [ ] Exit and cancellation seem to block
    - [ ] branching flow doesn't seem to have worked
    - [ ] add context.Context from msg.Context() to simple node at least
      - [ ] pass context to flow router on creation (?) 
    - [ ] delay node test: 

- [x] Make a redis version of the examples
- [x] Add a websocket observer output with streaming events
- [ ] Make sure the framework actually works

- [ ] Use worktree-tui to checkout a workspace with geppetto and pocketflow
- [ ] Add a real tool-calling LLM node
- [ ] Add nodes to extract structured data from LLM calls, etc...
