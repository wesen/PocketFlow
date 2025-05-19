package guardrail

import (
	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/impl"
	"strings"
)

// Ensure implementations
var _ core.SimpleNodeHandler = (*InputHandler)(nil)
var _ core.SimpleNodeHandler = (*GuardrailHandler)(nil)
var _ core.SimpleNodeHandler = (*LLMHandler)(nil)

type InputHandler struct{}

func (h *InputHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	return "What travel question do you have?", nil
}

func (h *InputHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	return "Plan my trip to Thailand", nil
}

func (h *InputHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	ctx.SharedData["user_input"] = execResult.(string)
	return "validate", nil, nil
}

type GuardrailHandler struct{}

func (h *GuardrailHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	input, _ := ctx.SharedData["user_input"].(string)
	return input, nil
}

func (h *GuardrailHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	input := prepResult.(string)
	valid := strings.Contains(strings.ToLower(input), "travel") || strings.Contains(strings.ToLower(input), "trip")
	return valid, nil
}

func (h *GuardrailHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	if execResult.(bool) {
		return "process", nil, nil
	}
	return "retry", nil, nil
}

type LLMHandler struct{}

func (h *LLMHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	input, _ := ctx.SharedData["user_input"].(string)
	return input, nil
}

func (h *LLMHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	return "Here is travel advice using mock LLM", nil
}

func (h *LLMHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	return "continue", nil, nil
}

func CreateGuardrailFlow() core.Flow {
	inputNode := impl.NewNode("input", map[string]interface{}{})
	guardNode := impl.NewNode("guard", map[string]interface{}{})
	llmNode := impl.NewNode("llm", map[string]interface{}{})

	flow := impl.NewFlowBuilder("guardrail").
		Begin(inputNode).
		Then(guardNode).
		On("process").Then(llmNode).
		On("retry").Then(inputNode).
		From(llmNode).On("continue").Then(inputNode).
		Build()

	return flow
}
