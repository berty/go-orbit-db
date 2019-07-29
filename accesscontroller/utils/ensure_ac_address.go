// utils is a package containing tools related to access controllers
package utils

import (
	"path"
	"strings"
)

// EnsureAddress Checks that an access controller address is properly formatted
func EnsureAddress(address string) string {
	parts := strings.Split(address, "/")
	suffix := parts[len(parts)-1]
	if suffix == "_access" {
		return address
	}

	return path.Join(address, "/_access")
}
