# Codex quota waiting

To keep a request pending instead of returning Codex's terminal
`usage_limit_reached` error when all usable accounts are exhausted:

```yaml
upstream:
  codex:
    wait-on-quota: true
    stream-bootstrap-buffering: true
    quota-recheck-interval: "15s"
requests:
  streaming:
    keepalive-seconds: 15
    send-keepalive-before-ft: true
  nonstream-keepalive-interval: 15
```

`wait-on-quota` defaults to false. It applies to local Codex generation and
token-count requests, not Home-managed dispatch. Ordinary request retry limits
and maximum retry intervals do not limit quota waiting. Client cancellation
still cancels the request; there is no server-side quota-wait timeout.

Waiting does not issue repeated generation requests. It rechecks local state,
wakes at the earliest eligible quota reset, and wakes immediately when a quota
probe verifies restoration or a credential update/addition makes capacity
available. Read-only usage probes run at the configured fixed interval, with
at most four concurrent probes; healthy accounts are not polled. There is no
exponential backoff added to request resumption. Eligibility still respects
model support, aliases, pinned credentials, and credential policies.

The authenticated management dashboard displays out-of-band quota warnings and
resumption notices through `/v8/management/observability/runtime-events`
(`type: codex.quota`, statuses `waiting`, `resuming`, `cancelled`, and
`reconnect_required`). Open the dashboard to receive these live notices. They
are not assistant messages and never alter model history. The downstream API
client receives keepalives, not a terminal quota error, while waiting. A generic
API client, including T3 Code, will not display the dashboard warning unless it
separately integrates this operator event channel.

Already-delivered output cannot safely be replayed. Established Codex duplex
WebSockets retain their reconnect/full-context signal; partial HTTP/SSE streams
receive a retryable `server_error` rather than the terminal provider quota code.
The original quota failure and reset timing remain in internal accounting and
logs. Other errors (authentication, policy, malformed requests, and transport
failures) retain their normal handling. WebSocket Ping keepalives run during auth selection and stream bootstrap, including
quota waiting, as well as during forwarding. They use `keepalive-seconds` and do
not depend on the SSE-only `send-keepalive-before-ft` option. No synthetic model
events or assistant messages are emitted while waiting. A client or an
intermediate proxy can still impose its own total request timeout; keepalives
cannot override such a limit.

Disabling waiting while a request is pending restores ordinary error handling.
Removing or disabling its eligible credentials triggers fresh selection rather
than returning an old quota error.
