# Classic Concurrency Problems

[![Go version](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go)](https://go.dev/)

Go implementations of classic concurrency problems. The examples use
goroutines, channels, synchronization, and bounded buffers to make common
concurrency patterns and failure modes easier to study.

## Why this project is useful

This repository provides small, runnable examples for:

- Understanding the **dining philosophers** problem and shared-resource coordination.
- Comparing different ways to prevent circular wait:
  - `base`: coordinates fork ownership through channels.
  - `broken-simmetry`: varies fork acquisition order between philosophers.
  - `dining-limits`: limits the number of philosophers attempting to dine at once.
- Comparing two **producer-consumer** implementations:
  - `channels`: uses a buffered channel as the shared queue.
  - `semaphore`: uses channels as `not-full` and `not-empty` semaphores around an
    explicit bounded buffer.
- Observing graceful shutdown, work counters, timeouts, and goroutine coordination
  in a compact codebase.

These programs are intended for learning and experimentation, not as drop-in
concurrency primitives for production systems.

## Prerequisites

- [Go 1.27 or newer](https://go.dev/dl/)
- A terminal capable of running Go commands

The module has no third-party Go dependencies.

## Getting started

Clone the repository and enter its directory:

```sh
git clone https://github.com/brunopedralli/classic-concurrency-problems.git
cd classic-concurrency-problems
```

Download module metadata and verify that all packages compile:

```sh
go mod download
go test ./...
```

There are no automated tests at present, so `go test ./...` primarily checks that
the example packages build successfully.

### Running an example

Each example is a standalone `main` package. Run one by specifying its directory:

```sh
go run ./dining-philosophers/base
go run ./dining-philosophers/broken-simmetry
go run ./dining-philosophers/dining-limits

go run ./producer-consumer/channels
go run ./producer-consumer/semaphore
```

The programs print their activity to standard output. The producer-consumer
examples run for a short, fixed interval; the dining-philosophers examples
perform repeated thinking and eating cycles, so they can take longer. Press
`Ctrl+C` to stop an example.

Constants near the top of each `main.go` control the number of workers, buffer
capacity, iteration count, and timing values. Changing them is a convenient way
to explore contention and scheduling behavior:

```go
const P = 2 // number of producers
const C = 5 // number of consumers
const K = 3 // shared buffer size
```

## Project layout

```text
.
├── dining-philosophers/
│   ├── base/              # Basic fork acquisition
│   ├── broken-simmetry/   # Alternating fork acquisition order
│   └── dining-limits/     # Limits philosophers dining concurrently
├── producer-consumer/
│   ├── channels/          # Buffered channel as the queue
│   └── semaphore/         # Explicit bounded buffer and semaphores
├── .github/workflows/     # Go and Markdown lint workflows
├── go.mod
└── CODEOWNERS
```

## Contributing

Contributions from learners are welcome. A typical workflow is:

1. Fork the repository and create a focused branch.
2. Make the smallest change that improves an example or its documentation.
3. Run `go test ./...`, format changed Go files, and run the relevant example.
4. Describe the concurrency concept or behavior changed in your pull request.


## Maintainer

[@brunopedralli](https://github.com/brunopedralli)
