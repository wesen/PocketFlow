# Branching Intent Flow

Demonstrates branching logic using an intent classifier. Depending on the detected
intent, the flow routes to different handlers.

## Run It

From the `go` directory:

```bash
go run main.go --flow=branching
```

## How It Works

```mermaid
flowchart TD
    input[User Input] --> classifier[Intent Classifier]
    classifier -->|weather_intent| weather[Weather]
    classifier -->|time_intent| time[Time]
    classifier -->|help_intent| help[Help]
    classifier -->|general_intent| general[General]
```

- **Intent Classifier** – examines the user's request.
- **Weather/Time/Help/General** – provide different responses based on the intent.
