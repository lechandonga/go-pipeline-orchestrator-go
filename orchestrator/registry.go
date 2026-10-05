package orchestrator

import "sync"

// Registry 是只在运行前使用的作业注册表。Register 可并发调用，
// 但 Build 之后注册将被拒绝（ReasonJobAlreadyBuilt）。
type Registry struct {
	mu     sync.Mutex
	specs  []*JobSpec
	byName map[string]*JobSpec
	built  bool
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{byName: make(map[string]*JobSpec)}
}

// Register 注册一个作业。重复注册返回 ReasonDuplicateJob。
func (r *Registry) Register(spec JobSpec) error {
	if spec.Name == "" {
		return &ValidationError{Reason: ReasonDuplicateJob, Detail: "job name must not be empty"}
	}
	if spec.Run == nil {
		return &ValidationError{Reason: ReasonDuplicateJob, Job: spec.Name, Detail: "run function must not be nil"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.built {
		return &ValidationError{Reason: ReasonJobAlreadyBuilt, Job: spec.Name, Detail: "registry has already been built"}
	}
	if _, ok := r.byName[spec.Name]; ok {
		return &ValidationError{Reason: ReasonDuplicateJob, Job: spec.Name, Detail: "job registered more than once"}
	}
	cp := spec
	if cp.MaxAttempts < 0 {
		cp.MaxAttempts = 0
	}
	r.byName[spec.Name] = &cp
	r.specs = append(r.specs, &cp)
	return nil
}

// MustRegister 注册失败时 panic，便于在初始化期使用。
func (r *Registry) MustRegister(spec JobSpec) {
	if err := r.Register(spec); err != nil {
		panic(err)
	}
}

// validate 校验引用合法性、自依赖与环。必须在持锁语义下调用（这里自带加锁）。
func (r *Registry) validate() error {
	for _, s := range r.specs {
		seen := make(map[string]struct{}, len(s.Deps))
		for _, dep := range s.Deps {
			if dep == s.Name {
				return &ValidationError{Reason: ReasonSelfDependency, Job: s.Name, Detail: "job depends on itself"}
			}
			if _, ok := r.byName[dep]; !ok {
				return &ValidationError{Reason: ReasonUnknownDep, Job: s.Name, Detail: "dependency not registered: " + dep}
			}
			if _, dup := seen[dep]; dup {
				// 重复声明的依赖不影响正确性，但明确拒绝，避免用户误以为有特殊语义。
				return &ValidationError{Reason: ReasonUnknownDep, Job: s.Name, Detail: "duplicated dependency declaration: " + dep}
			}
			seen[dep] = struct{}{}
		}
	}

	// DFS 三色标记检测环：0=白, 1=灰（在当前递归栈上）, 2=黑（已完成）。
	color := make(map[string]int, len(r.specs))
	var stack []string
	var dfs func(name string) error
	dfs = func(name string) error {
		color[name] = 1
		stack = append(stack, name)
		for _, dep := range r.byName[name].Deps {
			switch color[dep] {
			case 0:
				if err := dfs(dep); err != nil {
					return err
				}
			case 1:
				// 从环上第一个灰色节点开始截取，形成可读的环路径。
				cycle := append([]string{}, stack[indexOf(stack, dep):]...)
				cycle = append(cycle, dep)
				return &ValidationError{
					Reason: ReasonCycleDetected,
					Job:    dep,
					Detail: "dependency cycle: " + joinNames(cycle),
				}
			}
		}
		color[name] = 2
		stack = stack[:len(stack)-1]
		return nil
	}
	for _, s := range r.specs {
		if color[s.Name] == 0 {
			if err := dfs(s.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func indexOf(xs []string, target string) int {
	for i, x := range xs {
		if x == target {
			return i
		}
	}
	return -1
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i > 0 {
			out += " -> "
		}
		out += n
	}
	return out
}
