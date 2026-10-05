package orchestrator

import (
	"context"
	"errors"
)

func errorAs(err error, target any) bool { return errors.As(err, target) }

// isContextDone 判断错误链上是否包含取消/超时信号。
func isContextDone(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
