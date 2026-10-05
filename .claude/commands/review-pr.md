---
description: Review a GitHub pull request for code quality, patterns, and best practices
model: opus
argument-hint: "PR URL or number (e.g., 5 or https://github.com/zoolutions/pgbus/pull/5)"
---

# PR Review

Review PR for pattern compliance and issues. Be concise.

## Workflow

1. Fetch PR details and diff via `mcp__github__pull_request_read`
2. Categorize files by type
3. Check for pattern violations
4. Output structured review

## Pattern Violations to Check

```ruby
# WRONG -> RIGHT
Direct PGMQ calls outside Client   -> Use Pgbus::Client wrapper
Raw SQL in controllers/views        -> Use Web::DataSource
Hardcoded queue names               -> Use config.queue_name()
Missing visibility timeout          -> Always pass vt: parameter
Skip dead letter routing            -> Check read_ct > max_retries
No error handling in workers        -> Rescue and log via Pgbus.logger
Polling without LISTEN/NOTIFY       -> Use enable_notify_insert
Missing queue prefix                -> All queues go through config.queue_name
Thread.new for concurrency          -> Use Concurrent::* primitives
rescue StandardError => nil         -> Specific error handling
```

## Output Format

```
## Files Requiring Manual Review

| File | Reason |
|------|--------|
| lib/pgbus/process/worker.rb | Worker recycling logic, verify thresholds |
| lib/pgbus/client.rb | PGMQ interaction, check thread safety |

## Critical Issues

- `lib/pgbus/client.rb:45` - Missing mutex synchronization
- `lib/pgbus/process/worker.rb:12` - Memory check not platform-aware

## Suggestions (non-blocking)

- Consider extracting X to shared module

## Verdict

**Request Changes** - Fix thread safety before merge
```

## Tools

```
mcp__github__pull_request_read
  method: "get"        -> PR details
  method: "get_diff"   -> Changes
  method: "get_files"  -> File list
  method: "get_status" -> CI status

bundle exec rubocop    -> Style checks
bundle exec rspec      -> Tests
```
