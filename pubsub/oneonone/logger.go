package oneonone

import "go.uber.org/zap"

func logger() *zap.Logger {
	return zap.L().Named("orbitdb.pubsub.oneonone")
}
