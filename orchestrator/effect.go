package orchestrator

import "sync"

// EffectGuard 保证一段有副作用的逻辑在多次重试中只真正生效一次。
// 典型用法：作业可能被重试多次，但“扣款/写文件/发通知”只能发生一次；
// 首次调用 Do 时执行 fn，后续重试直接复用首次结果。
//
// 注意：如果运行被取消/超时，fn 内部也必须尊重 ctx 自行中断；
// Do 不替代取消传播。
type EffectGuard struct {
	mu      sync.Mutex
	done    bool
	onceErr error
}

// NewEffectGuard 创建幂等守卫。
func NewEffectGuard() *EffectGuard { return &EffectGuard{} }

// Do 首次调用时执行 fn 并缓存其 error；之后的调用（含其它 goroutine）
// 不会重复执行 fn，直接返回缓存结果。
func (g *EffectGuard) Do(fn func() error) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.done {
		return g.onceErr
	}
	g.done = true
	g.onceErr = fn()
	return g.onceErr
}

// Done 返回副作用是否已经生效。
func (g *EffectGuard) Done() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.done
}
