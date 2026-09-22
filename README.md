# tt-lib-go

Shared library for the Go services of the train ticket system. It gives every
service the same common base, so the infrastructure logic is not rewritten 20
times:

| Package      | What it solves                                                                          |
| ------------ | ---------------------------------------------------------------------------------------- |
| `config`     | Reads the service configuration from the environment, with development defaults.           |
| `events`     | Publish and consume messages with a single API, whether the transport is Kafka or RabbitMQ — the channel decides by its prefix (`kafka:...` / `rabbitmq:...`), not the service. |
| `health`     | The `GET /health` handler shared by every service.                                       |
| `httpclient` | A uniform HTTP client for service-to-service calls.                                      |

Same shape as its siblings [`tt-lib-node`](https://github.com/lucas-test-repos/tt-lib-node)
and [`tt-lib-py`](https://github.com/lucas-test-repos/tt-lib-py) — a publisher
with `publish`/`close`, a consumer with `start`/`close`, and a factory for the
health handler — so a developer who knows one recognizes it in the other two.

## Why it is public

The 69 Go, Node and Python services of the system — the 70 of the generated
tree minus the frontend, which uses no library — depend on these three
libraries by their version tag (`v0.1.0`): 24 in Go, 25 in Node and 20 in
Python. Publishing them as **public** repositories is what lets the CI of each
one of those 69 services resolve the dependency with no credential at all. It
is the only deliberate visibility asymmetry in the whole set of repositories.

## Usage

```go
import "github.com/lucas-test-repos/tt-lib-go/config"
import "github.com/lucas-test-repos/tt-lib-go/events"
import "github.com/lucas-test-repos/tt-lib-go/health"
import "github.com/lucas-test-repos/tt-lib-go/httpclient"
```

## Development

```bash
go build ./...
go test ./...
```
