package y

import (
	"syscall"
)

func init() {
	datasyncFileFlag = syscall.O_SYNC
}
