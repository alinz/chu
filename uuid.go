package chu

import (
	"github.com/rs/xid"
)

func uuid() string {
	return xid.New().String()
}
