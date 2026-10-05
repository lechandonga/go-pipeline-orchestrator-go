package orchestrator

import "fmt"

// ValidationReason 描述注册期/构建期被拒绝的可区分原因。
type ValidationReason string

const (
	ReasonDuplicateJob    ValidationReason = "duplicate_job"      // 重复注册
	ReasonUnknownDep      ValidationReason = "unknown_dependency" // 引用了不存在的作业
	ReasonSelfDependency  ValidationReason = "self_dependency"    // 作业依赖自身
	ReasonCycleDetected   ValidationReason = "cycle_detected"     // 依赖成环
	ReasonJobAlreadyBuilt ValidationReason = "job_already_built"  // 构建后再次注册
)

// ValidationError 在开始执行前返回，携带机器可读的原因与上下文。
type ValidationError struct {
	Reason ValidationReason
	Job    string // 相关作业名（可能为空）
	Detail string
}

func (e *ValidationError) Error() string {
	if e.Job != "" {
		return fmt.Sprintf("orchestrator: validation failed (%s) on job %q: %s", e.Reason, e.Job, e.Detail)
	}
	return fmt.Sprintf("orchestrator: validation failed (%s): %s", e.Reason, e.Detail)
}

// RetryableError 将一个错误标记为“可重试”。作业返回普通 error 视为不可重试，
// 只有显式包装为 RetryableError 的失败才会触发退避重试。
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	if e.Err == nil {
		return "retryable error"
	}
	return "retryable: " + e.Err.Error()
}

func (e *RetryableError) Unwrap() error { return e.Err }

// Retryable 包装一个错误，声明该失败可以按退避策略重试。
func Retryable(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err}
}

// IsRetryable 判断错误是否为可重试错误。context.Canceled / context.DeadlineExceeded
// 即使被包装也不会重试，因为此时整个运行正在终止。
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if isContextDone(err) {
		return false
	}
	var re *RetryableError
	return errorAs(err, &re)
}
