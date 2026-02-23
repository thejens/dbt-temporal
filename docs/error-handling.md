# Error Handling

| Error Type | Retryable? | Examples |
|-----------|-----------|----------|
| `Compilation` | No | Bad SQL, missing ref, Jinja render failure |
| `Configuration` | No | Missing profile, bad config, invalid selector |
| `Adapter` | Yes | Connection timeout, rate limit, transient DB error |
| `TestFailure` | No | A dbt test query returned failing rows (data won't change on retry) |
| `ProjectNotFound` | No | Worker doesn't have the requested project loaded |

## Retry Configuration

The `execute_node` activity retries transient adapter errors with exponential backoff. Defaults can be overridden in `dbt_temporal.yml`:

```yaml
retry:
  max_attempts: 3            # total attempts (1 = no retries)
  initial_interval_secs: 5   # first backoff delay
  backoff_coefficient: 2.0   # multiplier for successive backoffs
  max_interval_secs: 60      # upper bound on backoff delay
  non_retryable_errors:       # regex patterns — matching adapter errors won't be retried
    - "permission denied"
    - "relation .* does not exist"
    - "access denied"
```

All fields are optional and default to the values shown above (except `non_retryable_errors` which defaults to empty). The `non_retryable_errors` patterns are matched against the full adapter error message — if any pattern matches, the error is treated as non-retryable regardless of its type.
