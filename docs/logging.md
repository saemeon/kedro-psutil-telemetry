# Logging

Kedro's default logging configuration only enables `INFO`-level output for the `kedro` logger namespace. Since `kedro_psutil_telemetry` lives in a separate namespace, its metrics are silently dropped unless you explicitly enable them.

Add a `conf/logging.yml` to your project (Kedro picks this up automatically):

```yaml
version: 1

disable_existing_loggers: False

handlers:
  rich:
    class: kedro.logging.RichHandler
    rich_tracebacks: True

loggers:
  kedro:
    level: INFO
  kedro_psutil_telemetry:
    level: INFO

root:
  handlers: [rich]
```

Without this file you will see the hook registered in `kedro info` but no telemetry output during `kedro run`. This is not a bug in the hook — it is a consequence of how Kedro scopes its default logging.

!!! note
    If you use a custom `conf/logging.yml` already, just add `kedro_psutil_telemetry: {level: INFO}` under the `loggers` key.
