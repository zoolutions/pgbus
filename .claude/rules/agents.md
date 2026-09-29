# Agent Orchestration Rules

## Available Agents

| Agent | Purpose | When to Use |
|-------|---------|-------------|
| Explore | Codebase exploration | Finding files, understanding patterns |
| Plan | Implementation planning | Complex features, architectural decisions |
| general-purpose | Multi-step tasks | Research, complex searches |

## Immediate Agent Usage

Use agents PROACTIVELY without waiting for user prompt:

1. **Complex feature requests** -> Use Plan agent first (`model: sonnet`)
2. **Codebase exploration** -> Use Explore agent (`model: haiku`)
3. **Multi-file searches** -> Use Explore agent (`model: haiku`; not direct Glob/Grep)
4. **Architectural decisions** -> Use Plan agent (`model: sonnet`), and consult the advisor before committing to one

## Parallel Execution

**ALWAYS** use parallel Task execution for independent operations:

```markdown
# GOOD: Parallel execution
Launch multiple agents simultaneously:
1. Agent 1: Explore client/adapter patterns
2. Agent 2: Check event bus patterns
3. Agent 3: Review test coverage

# BAD: Sequential when unnecessary
First explore, wait, then check patterns, wait, then review...
```

## When to Use Explore Agent

Use the Explore agent (subagent_type=Explore, `model: haiku`) instead of direct Glob/Grep when:
- Open-ended codebase exploration
- Searching for patterns across client, adapter, event bus, and process layers
- Answering questions about codebase structure
- Finding related implementations across modules

## When NOT to Use Agents

Use direct tools when:
- Reading a specific known file path
- Simple pattern match in known location
- Single-file edits
- Running specific commands
