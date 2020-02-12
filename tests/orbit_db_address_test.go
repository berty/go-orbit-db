package tests

import (
	"testing"

	"berty.tech/go-orbit-db/address"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestOrbitDbAddress(t *testing.T) {
	//.createInstance(ipfs, { directory: path.join(dbPath, '1') })
	Convey("orbit-db - OrbitDB Address", t, FailureHalts, func(c C) {
		c.Convey("Parse Address", FailureHalts, func(c C) {
			c.Convey("throws an error if address is empty", FailureHalts, func(c C) {
				result, err := address.Parse("")
				assert.Nil(t, result)
				assert.NotNil(t, err)
				assert.Contains(t, err.Error(), "not a valid OrbitDB address")
			})

			c.Convey("parse address successfully", FailureHalts, func(c C) {
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

		c.Convey("isValid Address", FailureHalts, func(c C) {
			c.Convey("returns false for empty string", FailureHalts, func(c C) {
				err := address.IsValid("")
				assert.NotNil(t, err)
			})

			c.Convey("validate address successfully", FailureHalts, func(c C) {
				testAddr := "/orbitdb/bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi/first-database"
				err := address.IsValid(testAddr)

				assert.NoError(t, err)
			})

			c.Convey("handle missing orbitdb prefix", FailureHalts, func(c C) {
				testAddr := "bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi/first-database"
				err := address.IsValid(testAddr)

				assert.NoError(t, err)
			})

			c.Convey("handle missing db address name", FailureHalts, func(c C) {
				testAddr := "bafyreieecvmpthaoyasxzhnew2d25uaebwldeokea2wigyq5wr4dwiaimi"
				err := address.IsValid(testAddr)

				assert.NoError(t, err)
			})

			c.Convey("handle invalid multihash", FailureHalts, func(c C) {
				testAddr := "/orbitdb/Qmdgwt7w4uBsw8LXduzCd18zfGXeTmBsiR8edQ1hSfzc/first-database"
				err := address.IsValid(testAddr)

				assert.NotNil(t, err)
			})
		})
	})
}
