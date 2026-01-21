<h1 align="center">
  <br>
  go-orbit-db
  <br>
</h1>

<h3 align="center">🤝 Go version of orbit-db.</h3>

<p align="center">
  <a href="https://github.com/berty/go-orbit-db/actions?query=workflow%3AGo"><img src="https://github.com/berty/go-orbit-db/workflows/Go/badge.svg" /></a>
  <a href="https://github.com/berty/go-orbit-db/actions?query=workflow%3ARelease"><img src="https://github.com/berty/go-orbit-db/workflows/Release/badge.svg" /></a>
  <a href="https://www.codefactor.io/repository/github/berty/go-orbit-db">
    <img src="https://www.codefactor.io/repository/github/berty/go-orbit-db/badge"
         alt="Code Factor">
  </a>
  <a href="https://goreportcard.com/report/github.com/stateless-minds/go-orbit-db">
    <img src="https://goreportcard.com/badge/github.com/stateless-minds/go-orbit-db"
         alt="Go Report Card">
  </a>
  <a href="https://github.com/berty/go-orbit-db/releases">
    <img src="https://badge.fury.io/gh/berty%2Fgo-orbit-db.svg"
         alt="GitHub version">
  </a>
  <a href="https://codecov.io/gh/berty/go-orbit-db">
    <img src="https://codecov.io/gh/berty/go-orbit-db/branch/master/graph/badge.svg"
         alt="Coverage" />
  </a>
  <a href="https://godoc.org/github.com/stateless-minds/go-orbit-db">
    <img src="https://godoc.org/github.com/stateless-minds/go-orbit-db?status.svg"
         alt="GoDoc">
  </a>
</p>

<p align="center"><b>
    <a href="https://berty.tech">berty.tech</a> •
    <a href="https://github.com/berty">GitHub</a>
</b></p>

> A P2P Database on IPFS.

[orbit-db](https://github.com/orbitdb/orbit-db/) is a distributed peer-to-peer database on [IPFS](https://github.com/ipfs/ipfs). This project intends to provide a fully compatible port of the JavaScript version in Go.

The majority of this code was vastly derived from the JavaScript's [orbit-db](https://github.com/orbitdb/orbit-db) project.

## Usage

See [GoDoc](https://godoc.org/github.com/berty/go-orbit-db).

## Install

Constraints:

* `go-orbit-db` currently only works with **go1.16** and later
* You need to use the canonical import: `github.com/stateless-minds/go-orbit-db` instead of `github.com/berty/go-orbit-db`
* If you have `410 gone` errors, make sure that you use a reliable `$GOPROXY` or disable it completely

Example:

```console
$ go version
go version go1.17.3 darwin/amd64
$ go get github.com/stateless-minds/go-orbit-db
[...]
$
```

## Licensing

*go-orbit-db* is licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE) for the full license text.
