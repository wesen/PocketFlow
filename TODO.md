- [ ] Continue making sure that the observer is not a publisher proxy, but is just registering as subscriber
    - [ ] make sure that the actual implementation is not brain broken
    - [ ] I think the observability system should run its own router/runner and each new flow run should create its own as well
    - [ ] RunHandlers doesn't need to run in its own subroutine and I don't think it's needed anyway (see preivous point)
    - [ ] Exit and cancellation seem to block
    - [ ] branching flow doesn't seem to have worked
    - [ ] delay node test: 

2025-05-22T21:26:50-04:00 ERR Router error handling node message error="unsupported message type for delay node worker" handlerName=handle_delay_node messageID=04ee3e32-f6d6-4fc2-a716-6b9de8b966e6 nodeType=delay topic=node.delay
[watermill] 2025/05/22 21:26:50.632757 router.go:777: 	level=ERROR msg="Handler returned error" err="unsupported message type for delay node worker" 
^Z2025-05-22T21:26:50-04:00 DBG Router received message for node 


- [ ] Make a redis version of the examples
- [ ] Add a websocket observer output with streaming events
- [ ] Make sure the framework actually works
- [ ] Use worktree-tui to checkout a workspace with geppetto and pocketflow
- [ ] Add a real tool-calling LLM node
- [ ] Add nodes to extract structured data from LLM calls, etc...
