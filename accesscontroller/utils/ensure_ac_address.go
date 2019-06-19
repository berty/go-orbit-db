package utils

import (
	"path"
	"strings"
)

func EnsureAddress(address string) string {
	parts := strings.Split(address, "/")
	suffix := parts[len(parts)-1]
	if suffix == "_access" {
		return address
	}

	return path.Join(address, "/_access")
}
