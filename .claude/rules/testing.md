# Testing Rules

## TDD Workflow

Follow RED -> GREEN -> REFACTOR:

1. **RED**: Write a failing test first
2. **GREEN**: Write minimal code to pass
3. **REFACTOR**: Improve code while keeping tests green

## Coverage Requirements

- **80% minimum** for all code
- **100% required** for:
  - ActiveJob adapter (enqueue, execute, DLQ routing)
  - Event bus (handler, registry, publisher)
  - Client (PGMQ wrapper)
  - Configuration
  - Web::DataSource
  - Web::Authentication

## Test Type Preference

| Feature involves | Use |
|-----------------|-----|
| Configuration / Event / Serializer | Unit spec |
| Client / PGMQ interaction | Unit spec with mocked PGMQ |
| ActiveJob adapter | Integration spec |
| Worker / Supervisor | Unit spec (process behavior) |
| Dashboard DataSource | Unit spec with mocked client |
| Dashboard Authentication | Unit spec |
| Helper formatting | Unit spec |
| Dashboard view / CSS / JS | System spec (light + dark) + axe gate — see `.claude/rules/dashboard.md` |

## RSpec Conventions

```ruby
# Use let for setup
let(:config) { Pgbus::Configuration.new }

# Use subject for the thing being tested
subject(:data_source) { described_class.new(client: mock_client) }

# Use contexts for scenarios
context "when queue exists" do
  before { allow(mock_client).to receive(:metrics).and_return(metrics) }
  it { expect(result).not_to be_nil }
end

# Use doubles for PGMQ (avoid DB dependency in unit tests)
let(:mock_client) { double("Pgbus::Client", pgmq: double("pgmq")) }
```

## PGMQ in Tests

- Mock `Pgbus::Client` in unit tests -- PGMQ requires a real PostgreSQL connection
- Use doubles for PGMQ message objects (they're `Data.define` value objects)
- Test configuration without PGMQ connection
- Integration tests (when added) will need a real PostgreSQL with PGMQ extension

## Test Checklist

- [ ] Tests written BEFORE implementation
- [ ] All tests pass: `bundle exec rspec`
- [ ] Coverage meets requirements
- [ ] No skipped tests without reason
- [ ] Edge cases covered (nil messages, expired VT, max retries exceeded)
- [ ] Error paths tested (connection failures, deserialization errors)
- [ ] Dashboard change: `spec/system/accessibility_spec.rb` green, light + dark screenshots attached to the PR
