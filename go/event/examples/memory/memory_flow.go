package memory

import (
	"fmt"
	"github.com/The-Pocket/PocketFlow/go/event/core"
	"github.com/The-Pocket/PocketFlow/go/event/impl"
	"strings"
)

// Ensure handlers implement the interface
var _ core.SimpleNodeHandler = (*QuestionHandler)(nil)
var _ core.SimpleNodeHandler = (*RetrieveHandler)(nil)
var _ core.SimpleNodeHandler = (*AnswerHandler)(nil)
var _ core.SimpleNodeHandler = (*EmbedHandler)(nil)

type QuestionHandler struct{}

func (h *QuestionHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	// In this demo, we simply return a prompt string
	prompt := "What is your question?"
	if val, ok := ctx.Params["prompt"]; ok {
		if s, ok := val.(string); ok && s != "" {
			prompt = s
		}
	}
	return prompt, nil
}

func (h *QuestionHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	// For the example we simulate user input
	_ = prepResult.(string)
	return "Tell me about memory", nil
}

func (h *QuestionHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	question := execResult.(string)
	ctx.SharedData["last_question"] = question
	messages, _ := ctx.SharedData["messages"].([][2]string)
	ctx.SharedData["messages"] = messages
	return "retrieve", question, nil
}

type RetrieveHandler struct{}

func (h *RetrieveHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	question, _ := ctx.SharedData["last_question"].(string)
	archive, _ := ctx.SharedData["archive"].([][2]string)
	return map[string]interface{}{"q": question, "archive": archive}, nil
}

func (h *RetrieveHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	data := prepResult.(map[string]interface{})
	question := data["q"].(string)
	archive := data["archive"].([][2]string)

	for _, pair := range archive {
		if strings.Contains(strings.ToLower(pair[0]), strings.ToLower(question)) {
			return pair, nil
		}
	}
	return nil, nil
}

func (h *RetrieveHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	if pair, ok := execResult.([2]string); ok {
		ctx.SharedData["retrieved"] = pair
	}
	return "answer", nil, nil
}

type AnswerHandler struct{}

func (h *AnswerHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	question, _ := ctx.SharedData["last_question"].(string)
	retrieved, _ := ctx.SharedData["retrieved"].([2]string)
	return map[string]interface{}{"q": question, "retrieved": retrieved}, nil
}

func (h *AnswerHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	data := prepResult.(map[string]interface{})
	question := data["q"].(string)
	retrieved, _ := data["retrieved"].([2]string)

	answer := "This is a response using memory"
	if retrieved != [2]string{} {
		answer = fmt.Sprintf("Recalling '%s' then answering: %s", retrieved[0], question)
	}
	return answer, nil
}

func (h *AnswerHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	answer := execResult.(string)
	messages, _ := ctx.SharedData["messages"].([][2]string)
	messages = append(messages, [2]string{ctx.SharedData["last_question"].(string), answer})
	ctx.SharedData["messages"] = messages
	if len(messages) > 3 {
		archive, _ := ctx.SharedData["archive"].([][2]string)
		archive = append(archive, messages[0])
		ctx.SharedData["archive"] = archive
		ctx.SharedData["messages"] = messages[1:]
		return "embed", nil, nil
	}
	return "question", nil, nil
}

type EmbedHandler struct{}

func (h *EmbedHandler) Prep(ctx core.NodeContext) (interface{}, error) {
	return nil, nil
}

func (h *EmbedHandler) Exec(ctx core.NodeContext, prepResult interface{}) (interface{}, error) {
	return nil, nil
}

func (h *EmbedHandler) Post(ctx core.NodeContext, prepResult, execResult interface{}) (string, interface{}, error) {
	return "question", nil, nil
}

func CreateMemoryFlow() core.Flow {
	qNode := impl.NewNode("question", map[string]interface{}{})
	retrieveNode := impl.NewNode("retrieve", map[string]interface{}{})
	answerNode := impl.NewNode("answer", map[string]interface{}{})
	embedNode := impl.NewNode("embed", map[string]interface{}{})

	flow := impl.NewFlowBuilder("memory").
		Begin(qNode).
		Then(retrieveNode).
		Then(answerNode).
		On("embed").Then(embedNode).
		On("question").From(embedNode).Then(qNode).
		From(answerNode).On("question").Then(qNode).
		Build()

	return flow
}
