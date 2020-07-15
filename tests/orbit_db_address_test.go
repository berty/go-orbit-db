package tests

import (
	"strings"
	"testing"

	"berty.tech/go-orbit-db/address"
	"github.com/stretchr/testify/require"
)

func TestOrbitDBAddress(t *testing.T) {
	t.Run("Parse Address", func(t *testing.T) {
		t.Run("throws an error if address is empty", func(t *testing.T) {
			result, err := address.Parse("")
			require.Nil(t, result)
			require.Error(t, err)
			require.Contains(t, err.Error(), "not a valid OrbitDB address")
		})

		t.Run("parse address successfully", func(t *testing.T) {
			refAddr := "/orbitdb/bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi/first-database"
			result, err := address.Parse(refAddr)
			require.NoError(t, err)
			require.NotNil(t, result)

			require.Equal(t, result.GetRoot().String(), "bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi")
			require.Equal(t, result.GetPath(), "first-database")

			require.True(t, strings.HasPrefix(result.String(), "/orbitdb"))
			require.Contains(t, result.String(), "bafy")
		})
	})

	t.Run("isValid Address", func(t *testing.T) {
		t.Run("returns false for empty string", func(t *testing.T) {
			err := address.IsValid("")
			require.Error(t, err)
		})

		t.Run("validate address successfully", func(t *testing.T) {
			testAddr := "/orbitdb/bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi/first-database"
			err := address.IsValid(testAddr)
			require.NoError(t, err)
		})

		t.Run("handle missing orbitdb prefix", func(t *testing.T) {
			testAddr := "bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi/first-database"
			err := address.IsValid(testAddr)
			require.NoError(t, err)
		})

		t.Run("handle missing db address name", func(t *testing.T) {
			testAddr := "bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi"
			err := address.IsValid(testAddr)
			require.NoError(t, err)
		})

		t.Run("handle invalid multihash", func(t *testing.T) {
			testAddr := "/orbitdb/Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzc/first-database"
			err := address.IsValid(testAddr)
			require.Error(t, err)
		})
	})
}
