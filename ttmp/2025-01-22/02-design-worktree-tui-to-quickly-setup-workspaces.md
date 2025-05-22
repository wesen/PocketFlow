# Worktree TUI - Quick Workspace Setup Tool

## Overview

A Terminal User Interface (TUI) application that allows developers to quickly create Go workspaces by selecting from a predefined list of repositories and automatically setting up worktrees with `go.work` initialization.

## Purpose

- **Problem**: Setting up development workspaces with multiple related repositories is repetitive and error-prone
- **Solution**: Interactive TUI that automates the process of cloning repos, creating worktrees, and initializing Go workspaces
- **Target Users**: Go developers working with multiple related repositories (microservices, monorepo components, etc.)

## Core Features

### 1. Repository Selection Interface
- **Multi-select list** of available repositories from config
- **Search/filter** functionality to quickly find repos
- **Repository metadata display**: description, last updated, main branch
- **Dependency visualization**: show which repos commonly work together

### 2. Workspace Configuration
- **Workspace name input** with smart defaults (based on selected repos)
- **Target directory selection** with path validation
- **Branch selection** per repository (default to main/master)
- **Conflict detection** for existing directories

### 3. Execution & Progress
- **Real-time progress display** for each operation
- **Detailed logging** with expandable sections
- **Error handling** with retry options
- **Success summary** with next steps

## Configuration File Format

```yaml
# ~/.config/worktree-tui/config.yaml
workspaces:
  default_base_path: "~/code/workspaces"
  
repositories:
  - name: "pocketflow"
    description: "Minimalist LLM framework"
    url: "https://github.com/the-pocket/PocketFlow.git"
    local_path: "~/code/others/llms/PocketFlow"  # optional: use existing local repo
    default_branch: "main"
    tags: ["llm", "framework", "core"]
    
  - name: "geppetto"
    description: "Corporate headquarters automation"
    url: "git@github.com:wesen/corporate-headquarters.git"
    local_path: "~/code/wesen/corporate-headquarters"
    subdirectory: "geppetto"  # for monorepos
    default_branch: "main"
    tags: ["automation", "corporate"]
    
  - name: "ai-tools"
    description: "AI development utilities"
    url: "git@github.com:wesen/ai-tools.git"
    default_branch: "develop"
    tags: ["ai", "utilities"]

presets:
  - name: "LLM Development"
    description: "Full stack LLM development environment"
    repositories: ["pocketflow", "geppetto", "ai-tools"]
    
  - name: "Corporate Automation"
    description: "Corporate tools and automation"
    repositories: ["geppetto", "ai-tools"]

settings:
  git:
    default_remote: "origin"
    fetch_on_clone: true
  
  go:
    auto_init_workspace: true
    workspace_file_name: "go.work"
```

## User Interface Design

### Main Screen
```
┌─ Worktree TUI - Quick Workspace Setup ─────────────────────────────────┐
│                                                                         │
│ Select repositories for your workspace:                                 │
│                                                                         │
│ Search: [llm________________]                                           │
│                                                                         │
│ Presets:                                                                │
│ ○ LLM Development        ○ Corporate Automation                         │
│                                                                         │
│ Repositories:                                                           │
│ ☑ pocketflow            Minimalist LLM framework                       │
│ ☐ geppetto              Corporate headquarters automation               │
│ ☑ ai-tools              AI development utilities                       │
│                                                                         │
│ Workspace Configuration:                                                │
│ Name: [llm-workspace_______________]                                    │
│ Path: [~/code/workspaces/llm-workspace]                                │
│                                                                         │
│ [Create Workspace]  [Cancel]                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

### Progress Screen
```
┌─ Creating Workspace: llm-workspace ────────────────────────────────────┐
│                                                                         │
│ ✓ Creating workspace directory                                         │
│ ✓ Cloning pocketflow                                                    │
│ ⟳ Setting up ai-tools worktree...                                      │
│ ○ Initializing go.work                                                  │
│                                                                         │
│ Current: git worktree add ../ai-tools ~/code/ai-tools                  │
│                                                                         │
│ [View Logs]  [Cancel]                                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Technical Implementation

### Architecture
```
cmd/
├── main.go                 # CLI entry point
├── tui/
│   ├── app.go             # Main TUI application
│   ├── screens/
│   │   ├── selection.go   # Repository selection screen
│   │   ├── config.go      # Workspace configuration screen
│   │   └── progress.go    # Progress/execution screen
│   └── components/
│       ├── repolist.go    # Repository list component
│       ├── search.go      # Search/filter component
│       └── progress.go    # Progress indicator component
├── config/
│   ├── loader.go          # Configuration file loading
│   └── types.go           # Configuration data structures
├── workspace/
│   ├── manager.go         # Workspace creation logic
│   ├── git.go             # Git operations (clone, worktree)
│   └── golang.go          # Go workspace initialization
└── utils/
    ├── paths.go           # Path manipulation utilities
    └── validation.go      # Input validation
```

### Key Dependencies
- **TUI Framework**: [bubbletea](https://github.com/charmbracelet/bubbletea) + [lipgloss](https://github.com/charmbracelet/lipgloss)
- **Configuration**: [viper](https://github.com/spf13/viper) for YAML config loading
- **Git Operations**: [go-git](https://github.com/go-git/go-git) or shell commands
- **CLI Framework**: [cobra](https://github.com/spf13/cobra) for command structure

## Workflow Steps

### 1. Repository Selection
1. Load configuration file
2. Display repository list with search/filter
3. Allow multi-select with keyboard navigation
4. Show preset options for quick selection
5. Validate selection (at least one repo)

### 2. Workspace Configuration
1. Generate default workspace name from selected repos
2. Allow user to customize name and path
3. Validate target directory doesn't exist
4. Show branch selection for each repository
5. Preview final workspace structure

### 3. Workspace Creation
1. Create workspace base directory
2. For each repository:
   - Clone if not local, or use existing local repo
   - Create worktree in workspace directory
   - Handle subdirectory extraction if needed
3. Initialize `go.work` file with all Go modules
4. Display success summary with next steps

## Error Handling

### Common Scenarios
- **Network issues**: Retry with exponential backoff
- **Permission errors**: Clear error messages with suggested fixes
- **Existing directories**: Offer to backup/rename or choose different location
- **Git authentication**: Detect and guide through SSH key setup
- **Missing Go modules**: Skip go.work initialization with warning

### Recovery Options
- **Partial failure**: Continue with successful repos, report failures
- **Cleanup on abort**: Remove partially created workspace
- **Retry mechanisms**: Allow retrying failed operations

## Configuration Management

### Default Locations
1. `~/.config/worktree-tui/config.yaml`
2. `./worktree-tui.yaml` (project-specific)
3. Environment variable: `WORKTREE_TUI_CONFIG`

### Validation
- Repository URLs are accessible
- Local paths exist and are valid Git repositories
- Branch names exist in repositories
- No circular dependencies in presets

## Future Enhancements

### Phase 2 Features
- **Template support**: Custom workspace templates with additional setup scripts
- **IDE integration**: Auto-open in VS Code/GoLand with proper workspace configuration
- **Dependency management**: Automatic `go mod replace` directives for local development
- **Sync capabilities**: Update existing workspaces when repositories change

### Phase 3 Features
- **Team sharing**: Shared configuration repositories for team-wide workspace definitions
- **Docker integration**: Option to create containerized development environments
- **CI/CD integration**: Generate GitHub Actions workflows for workspace validation

## Success Metrics

- **Time savings**: Reduce workspace setup from 10+ minutes to <2 minutes
- **Error reduction**: Eliminate common setup mistakes (wrong branches, missing repos)
- **Adoption**: Team members actively use tool for new feature development
- **Maintenance**: Configuration stays up-to-date with repository changes 