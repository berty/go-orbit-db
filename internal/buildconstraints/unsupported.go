// +build !go1.13

package buildconstraints

func error() {
	`Unsupported go version, please use go1.13`
}

// this file is called for (version < go1.13)
// See https://golang.org/pkg/go/build/
