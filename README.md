<h1 align="center">
  <br>
  go-orbit-db
  <br>
</h1>

<h3 align="center">🤝 Go version of orbit-db.</h3>

<p align="center">
  <a href="https://circleci.com/gh/berty/go-orbit-db">
    <img src="https://circleci.com/gh/berty/go-orbit-db.svg?style=svg"
         alt="Build Status">
  </a>
  <a href="https://www.codefactor.io/repository/github/berty/go-orbit-db">
    <img src="https://www.codefactor.io/repository/github/berty/go-orbit-db/badge"
         alt="Code Factor">
  </a>
  <a href="https://goreportcard.com/report/berty.tech/go-orbit-db">
    <img src="https://goreportcard.com/badge/berty.tech/go-orbit-db"
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
  <a href="https://godoc.org/berty.tech/go-orbit-db">
    <img src="https://godoc.org/berty.tech/go-orbit-db?status.svg"
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

* `go-orbit-db` currently only works with **go1.12**

If you receive an error "Unsupported go version, please use go1.12",
please install go version 1.12:

```console
$ go get golang.org/dl/go1.12.14
$ go1.12.14 download
Downloaded   0.0% (    15173 / 126724102 bytes) ...
Downloaded   2.9% (  3668793 / 126724102 bytes) ...
Downloaded   8.3% ( 10517305 / 126724102 bytes) ...
Downloaded  14.1% ( 17890105 / 126724102 bytes) ...
Downloaded  19.8% ( 25099065 / 126724102 bytes) ...
Downloaded  25.5% ( 32308025 / 126724102 bytes) ...
Downloaded  31.4% ( 39811897 / 126724102 bytes) ...
Downloaded  37.0% ( 46922553 / 126724102 bytes) ...
Downloaded  42.1% ( 53410617 / 126724102 bytes) ...
Downloaded  47.7% ( 60390201 / 126724102 bytes) ...
Downloaded  53.1% ( 67238713 / 126724102 bytes) ...
Downloaded  59.1% ( 74873657 / 126724102 bytes) ...
Downloaded  65.5% ( 83032889 / 126724102 bytes) ...
Downloaded  71.6% ( 90766137 / 126724102 bytes) ...
Downloaded  77.2% ( 97811257 / 126724102 bytes) ...
Downloaded  82.5% (104594233 / 126724102 bytes) ...
Downloaded  87.9% (111344441 / 126724102 bytes) ...
Downloaded  93.1% (118001574 / 126724102 bytes) ...
Downloaded  98.4% (124719014 / 126724102 bytes) ...
Downloaded 100.0% (126724102 / 126724102 bytes)
Unpacking /home/username/sdk/go1.12.14/go1.12.14.linux-amd64.tar.gz ...
Success. You may now run 'go1.12.14'
```

After this, you may use "go.12.14" instead of "go" at the commandline.

* You need to use the canonical import: `berty.tech/go-orbit-db` instead of `github.com/berty/go-orbit-db`
* If you have `410 gone` errors, make sure that you use a reliable `$GOPROXY` or disable it completely

Example:

```console
$ go version
go version go1.12.10 linux/amd64
$ go get berty.tech/go-orbit-db
[...]
$
```

## Licensing

*go-orbit-db* is licensed under the Apache License, Version 2.0.
See [LICENSE](LICENSE) for the full license text.
