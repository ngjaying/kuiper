# Release Notes

## Security hardening

- REST API now binds to localhost by default.
- CLI service now binds to localhost by default.
- HTTP data ingestion server now binds to localhost by default.
- External file access is disabled by default.
- Production deployments exposing REST API should enable authentication.

> **Behavior change:** By default the REST management API, the CLI service,
> and the HTTP data ingestion server now listen on `127.0.0.1` only.
> Remote devices pushing data over HTTP, and remote CLI/REST clients, will no
> longer be able to connect unless operators explicitly set the relevant bind
> address (e.g. `0.0.0.0`) and enable authentication where appropriate.
