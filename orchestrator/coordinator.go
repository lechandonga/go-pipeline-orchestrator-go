package orchestrator

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// jobEvent 是作业 goroutine 回报给协调器的事件。所有作业状态转移都集中在
// 协调器这一个 goroutine 完成，保证并发运行下状态机没有数据竞争。
type jobEvent struct {
	name        string
	attemptDone bool  // false 表示退避结束、可重新派发
	attempt     int   // attemptDone=true 时对应的尝试序号
	err         error // attemptDone=true 时的执行结果
}

// coordinate 是单线程协调器：决定作业何时启动、重试、失败如何级联，
// 是“启动与结束顺序可解释”的唯一事实来源。
func (run *Run) coordinate() {
	eng := run.plan.engine
	events := make(chan jobEvent, len(run.order)*2)

	semCap := len(run.order)
	if c := eng.opts.Concurrency; c > 0 {
		semCap = c
	}
	sem := make(chan struct{}, semCap)

	inFlight := 0   // 正在执行 Run 的作业数（占槽）
	backingOff := 0 // 退避等待中的作业数（不占槽，状态仍为 running）
	ready := []string{}
	retried := map[string]int{} // name -> 已经发起的尝试次数

	launch := func(name string) {
		run.mu.Lock()
		defer run.mu.Unlock()
		jt := run.jobs[name]
		jt.state = JobRunning
		if jt.startedAt.IsZero() {
			jt.startedAt = eng.now()
		}
		retried[name]++
		jt.attempts = retried[name]
		run.attempts++
		inFlight++
		eng.logger.Printf("run %d job %q started (attempt %d/%d, in-flight=%d)",
			run.id, name, jt.attempts, maxInt(jt.spec.MaxAttempts, 1), inFlight)
		go run.executeAttempt(name, jt.attempts, sem, events)
	}

	// 初始入队：没有依赖的作业，按注册顺序。
	run.mu.Lock()
	for _, name := range run.order {
		if len(run.jobs[name].waitDeps) == 0 {
			run.jobs[name].state = JobReady
			ready = append(ready, name)
		}
	}
	run.mu.Unlock()

	dispatch := func() {
		for len(ready) > 0 {
			select {
			case sem <- struct{}{}:
				next := ready[0]
				ready = ready[1:]
				launch(next)
			default:
				return
			}
		}
	}

	drainCanceled := func() {
		eng.logger.Printf("run %d coordinator observed termination (%v), waiting for %d in-flight jobs",
			run.id, run.ctx.Err(), inFlight)
		for inFlight > 0 || backingOff > 0 {
			msg := <-events
			if !msg.attemptDone {
				backingOff--
				run.mu.Lock()
				jt := run.jobs[msg.name]
				if run.ctx.Err() != nil {
					jt.state = JobCanceled
					if jt.endedAt.IsZero() {
						jt.endedAt = eng.now()
					}
					jt.lastErr = run.ctx.Err().Error()
				}
				run.mu.Unlock()
				continue
			}
			inFlight--
			<-sem
			run.mu.Lock()
			jt := run.jobs[msg.name]
			jt.state = JobCanceled
			if jt.endedAt.IsZero() {
				jt.endedAt = eng.now()
			}
			jt.lastErr = msg.err.Error()
			run.skipCascadeLocked(msg.name, &ready)
			run.mu.Unlock()
			eng.logger.Printf("run %d job %q stopped by run termination: %v", run.id, msg.name, msg.err)
		}
		run.mu.Lock()
		run.markUnstartedSkippedLocked()
		run.mu.Unlock()
		run.finish()
	}

	for {
		dispatch()
		if inFlight == 0 && backingOff == 0 && len(ready) == 0 {
			break
		}

		select {
		case msg := <-events:
			if !msg.attemptDone {
				// 退避结束：若运行仍在进行，重新入队等待并发槽。
				backingOff--
				if run.ctx.Err() != nil {
					run.mu.Lock()
					cj := run.jobs[msg.name]
					cj.state = JobCanceled
					if cj.endedAt.IsZero() {
						cj.endedAt = run.plan.engine.now()
					}
					cj.lastErr = run.ctx.Err().Error()
					dummy := []string{}
					run.skipCascadeLocked(msg.name, &dummy)
					run.mu.Unlock()
					eng.logger.Printf("run %d job %q stopped while waiting in backoff", run.id, msg.name)
					continue
				}
				run.mu.RLock()
				st := run.jobs[msg.name].state
				run.mu.RUnlock()
				if st == JobRunning {
					ready = append(ready, msg.name)
					eng.logger.Printf("run %d job %q backoff finished, re-queued", run.id, msg.name)
				}
				continue
			}

			inFlight--
			<-sem

			if run.ctx.Err() != nil {
				// 终止信号后结束的尝试，统一走取消收尾。
				drainCanceled()
				return
			}
			run.mu.Lock()
			run.handleAttemptDoneLocked(msg, &ready, &backingOff, events)
			run.mu.Unlock()

		case <-run.ctx.Done():
			drainCanceled()
			return
		}
	}

	run.mu.Lock()
	run.markUnstartedSkippedLocked()
	run.mu.Unlock()
	run.finish()
}

// handleAttemptDone 处理一次执行回报：成功解锁下游；可重试错误进入退避；
// 其它错误使作业失败并级联跳过下游。
// 调用方必须持有 run.mu 写锁。
func (run *Run) handleAttemptDoneLocked(msg jobEvent, ready *[]string, backingOff *int, events chan<- jobEvent) {
	eng := run.plan.engine
	jt := run.jobs[msg.name]

	if msg.err == nil {
		jt.state = JobSucceeded
		jt.endedAt = eng.now()
		jt.lastErr = ""
		eng.logger.Printf("run %d job %q succeeded after %d attempt(s), duration=%s",
			run.id, msg.name, jt.attempts, jt.endedAt.Sub(jt.startedAt))
		run.unlockDownstreamLocked(msg.name, ready)
		return
	}

	jt.lastErr = msg.err.Error()

	if IsRetryable(msg.err) && msg.attempt < jt.spec.MaxAttempts {
		wait := run.backoff(jt.spec, msg.attempt)
		run.retries++
		*backingOff++
		eng.logger.Printf("run %d job %q attempt %d failed retryably: %v; backing off %s",
			run.id, msg.name, msg.attempt, msg.err, wait)
		go func(name string) {
			timer := time.NewTimer(wait)
			defer timer.Stop()
			select {
			case <-timer.C:
				events <- jobEvent{name: name}
			case <-run.ctx.Done():
				// 退避期间终止：发一个非完成事件通知协调器计数，
				// 协调器在 drain 阶段会把作业归为 canceled。
				events <- jobEvent{name: name, err: run.ctx.Err()}
			}
		}(msg.name)
		return
	}

	jt.state = JobFailed
	jt.endedAt = eng.now()
	if IsRetryable(msg.err) {
		eng.logger.Printf("run %d job %q failed: retries exhausted after %d attempt(s): %v",
			run.id, msg.name, jt.attempts, msg.err)
	} else {
		eng.logger.Printf("run %d job %q failed with non-retryable error after %d attempt(s): %v",
			run.id, msg.name, jt.attempts, msg.err)
	}
	run.skipCascadeLocked(msg.name, ready)
}

// executeAttempt 执行一次尝试并恰好回报一个 attemptDone 事件。
func (run *Run) executeAttempt(name string, attempt int, sem chan struct{}, events chan<- jobEvent) {
	err := run.jobs[name].spec.Run(run.ctx, attempt)
	events <- jobEvent{name: name, attemptDone: true, attempt: attempt, err: err}
}

// unlockDownstreamLocked 在作业成功后推进下游：仅当所有依赖均成功才入队。
func (run *Run) unlockDownstreamLocked(name string, ready *[]string) {
	for _, down := range run.plan.rev[name] {
		jt := run.jobs[down]
		if jt.state != JobPending {
			continue
		}
		delete(jt.waitDeps, name)
		if len(jt.waitDeps) == 0 {
			jt.state = JobReady
			*ready = append(*ready, down)
			run.plan.engine.logger.Printf("run %d job %q became ready", run.id, down)
		}
	}
}

// skipCascadeLocked 从失败/取消作业出发 BFS 跳过所有尚未启动的下游。
func (run *Run) skipCascadeLocked(root string, ready *[]string) {
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, down := range run.plan.rev[cur] {
			jt := run.jobs[down]
			if jt.state != JobPending && jt.state != JobReady {
				continue
			}
			jt.state = JobSkipped
			jt.endedAt = run.plan.engine.now()
			jt.lastErr = fmt.Sprintf("skipped: upstream %q did not succeed", cur)
			run.plan.engine.logger.Printf("run %d job %q skipped: upstream %q failed/canceled", run.id, down, cur)
			queue = append(queue, down)
		}
	}

	*ready = (*ready)[:0]
	for _, nm := range run.order {
		if run.jobs[nm].state == JobReady {
			*ready = append(*ready, nm)
		}
	}
}

// markUnstartedSkippedLocked 清理终止时仍处于 pending/ready 的作业。
func (run *Run) markUnstartedSkippedLocked() {
	for _, name := range run.order {
		jt := run.jobs[name]
		if jt.state == JobPending || jt.state == JobReady {
			jt.state = JobSkipped
			jt.endedAt = run.plan.engine.now()
			if jt.lastErr == "" {
				jt.lastErr = "skipped: run terminated before job started"
			}
		}
	}
}

// finish 计算整次运行终态、生成失败因果说明并关闭 done。
func (run *Run) finish() {
	run.mu.Lock()
	defer run.mu.Unlock()

	run.endTime = run.plan.engine.now()
	failed, canceled, succeeded, skipped := 0, 0, 0, 0
	for _, jt := range run.jobs {
		switch jt.state {
		case JobSucceeded:
			succeeded++
		case JobFailed:
			failed++
		case JobCanceled:
			canceled++
		case JobSkipped:
			skipped++
		}
	}

	switch {
	case run.timedOut.Load() || contextDeadline(run):
		run.state = RunTimedOut
	case run.ctx.Err() != nil:
		run.state = RunCanceled
	case failed > 0 || canceled > 0:
		run.state = RunFailed
	default:
		run.state = RunSucceeded
	}

	report := run.buildFailureReportLocked()
	run.plan.engine.logger.Printf("run %d finished: state=%s succeeded=%d failed=%d canceled=%d skipped=%d retries=%d attempts=%d duration=%s",
		run.id, run.state, succeeded, failed, canceled, skipped,
		run.retries, run.attempts, run.endTime.Sub(run.startTime))
	if report != "" {
		run.plan.engine.logger.Printf("run %d failure report:\n%s", run.id, strings.TrimSpace(report))
	}
	close(run.done)
}

func contextDeadline(run *Run) bool { return run.ctx.Err() == context.DeadlineExceeded }
