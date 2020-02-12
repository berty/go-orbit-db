package tests

import (
	"testing"

	"berty.tech/go-orbit-db/address"

	"github.com/stretchr/testify/assert"
)

func TestOrbitDbAddress(t *testing.T) {
	//.createInstance(ipfs, { directory: path.join(dbPath, '1') })
	t.Run("orbit-db - OrbitDB Address", func(t *testing.T) {
		t.Run("Parse Address", func(t *testing.T) {
			t.Run("throws an error if address is empty", func(t *testing.T) {
				result, err := address.Parse("")
				assert.Nil(t, result)
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), "not a valid OrbitDB address")
			})

			t.Run("parse address successfully", func(t *testing.T) {
				refAddr := "/orbitdb/bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi/first-database"
				result, err := address.Parse(refAddr)
				assert.NoError(t, err)
				assert.NotNil(t, result)

				assert.Equal(t, "bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi", result.GetRoot().String())
				assert.Equal(t, "first-database", result.GetPath())

				assert.Regexp(t, "^/orbitdb", result.String())
				assert.Contains(t, result.String(), "bafy")
			})
		})

		t.Run("isValid Address", func(t *testing.T) {
			t.Run("returns false for empty string", func(t *testing.T) {
				err := address.IsValid("")
				assert.NotNil(t, err)
			})

			t.Run("validate address successfully", func(t *testing.T) {
				testAddr := "/orbitdb/bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi/first-database"
				err := address.IsValid(testAddr)

				assert.NoError(t, err)
			})

			t.Run("handle missing orbitdb prefix", func(t *testing.T) {
				testAddr := "bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi/first-database"
				err := address.IsValid(testAddr)

				assert.NoError(t, err)
			})

			t.Run("handle missing db address name", func(t *testing.T) {
				testAddr := "bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi"
				err := address.IsValid(testAddr)

				assert.NoError(t, err)
			})

			t.Run("handle invalid multihash", func(t *testing.T) {
				testAddr := "/orbitdb/Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzc/first-database"
				err := address.IsValid(testAddr)

				assert.NotNil(t, err)
			})
		})
	})
}
