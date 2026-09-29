---
description: "Coordinates development across pgbus layers. Use when planning multi-layer features, orchestrating implementation order, or designing new subsystems."
model: opus
argument-hint: "feature or task to coordinate"
---

# Pgbus Architect Mode

You are now in **Architect Mode** - coordinating development across all pgbus layers.

## Why This Skill Exists

Pgbus spans multiple layers (PGMQ transport, ActiveJob adapter, event bus, process model, dashboard). Without coordination, developers tackle layers in the wrong order, miss integration points, or create inconsistent implementations.

## Pgbus Architecture Layers

```
Layer 6: Dashboard (Web UI)          app/controllers/pgbus/, app/views/pgbus/
Layer 5: CLI                         lib/pgbus/cli.rb
Layer 4: Process Model               lib/pgbus/process/ (supervisor, worker, dispatcher, consumer)
Layer 3: Event Bus                   lib/pgbus/event_bus/ (publisher, subscriber, registry, handler)
Layer 2: ActiveJob Adapter           lib/pgbus/active_job/ (adapter, executor)
Layer 1: Client                      lib/pgbus/client.rb (PGMQ wrapper)
Layer 0: Configuration               lib/pgbus/configuration.rb, config_loader.rb
```

## Typical Implementation Flow

1. **Configuration** - Add new config options if needed
2. **Client** - Add PGMQ operations to the client wrapper
3. **Adapter / Event Bus** - Build the feature's core logic
4. **Process Model** - Add worker/consumer support
5. **Dashboard** - Expose in the web UI via DataSource
6. **Tests** - Coverage across all touched layers

## When to Delegate vs. Do Directly

**Delegate when**:
- Multiple files across a layer need changes
- Deep domain expertise is needed (e.g., PGMQ internals)
- Work is clearly scoped to one layer

**Handle directly when**:
- Simple, single-file changes
- Cross-cutting concerns affecting multiple layers
- Quick fixes or minor adjustments

## Decision Guidelines

| Decision | Use When |
|----------|----------|
| New config option | Feature needs user-configurable behavior |
| New Client method | New PGMQ operation needed |
| New event handler | Business event needs processing |
| New worker type | Different processing pattern needed |
| Dashboard page | Users need visibility into new feature |
| Migration change | New metadata table or PGMQ queue |

## Integration Points

| When working on... | Also consider... |
|-------------------|------------------|
| Client changes | Worker/consumer that calls it |
| New queue type | Dead letter queue, dashboard visibility |
| Event bus feature | Idempotency table, subscriber registry |
| Process model | Heartbeat, supervisor restart logic |
| Dashboard | DataSource queries, authentication |
| Configuration | Config loader YAML, generator template |

## Common Mistakes to Avoid

| Wrong | Right |
|-------|-------|
| Start with dashboard | Start with client/adapter layer |
| Skip configuration | Make it configurable from the start |
| Direct PGMQ calls | Go through Client wrapper |
| Forget DLQ | Every queue needs a dead letter strategy |
| Skip tests | TDD -- tests first at every layer |
| Monolith methods | Small files, focused classes |

## Verification Checklist

- [ ] Implementation order planned (bottom-up)
- [ ] Dependencies between layers identified
- [ ] PGMQ operations go through Client
- [ ] Configuration is extensible
- [ ] Dashboard exposes new data via DataSource
- [ ] Tests cover all touched layers
- [ ] `bundle exec rubocop` passes
- [ ] `bundle exec rspec` passes

Run the `fable-validator` agent on the combined diff first; do not open or merge on BLOCK.

## Handoff

When complete, summarize:
- Implementation plan with layer order
- Files to create/modify per layer
- Integration points identified
- Architectural decisions made

Now, coordinate pgbus development with this architectural perspective.
