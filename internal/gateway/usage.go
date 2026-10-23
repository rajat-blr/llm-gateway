package gateway

// Pointer fields distinguish absent counters from a real zero. Partial usage
// is unknown and must never be interpreted as a zero-token provider result.
type reportedUsage struct {
	Prompt     *int `json:"prompt_tokens"`
	Completion *int `json:"completion_tokens"`
	Total      *int `json:"total_tokens"`
}

func (r *reportedUsage) normalized() (*Usage, error) {
	if r == nil {
		return nil, nil
	}
	for _, n := range []*int{r.Prompt, r.Completion, r.Total} {
		if n != nil && (*n < 0 || *n > 1_000_000_000) {
			return nil, errProtocol
		}
	}
	if r.Prompt == nil || r.Completion == nil || r.Total == nil {
		return nil, nil
	}
	u := &Usage{*r.Prompt, *r.Completion, *r.Total}
	if !u.valid() {
		return nil, errProtocol
	}
	return u, nil
}

type anthropicUsage struct {
	Input  *int `json:"input_tokens"`
	Output *int `json:"output_tokens"`
}

func (r *anthropicUsage) normalized() (*Usage, error) {
	if r == nil {
		return nil, nil
	}
	for _, n := range []*int{r.Input, r.Output} {
		if n != nil && (*n < 0 || *n > 1_000_000_000) {
			return nil, errProtocol
		}
	}
	if r.Input == nil || r.Output == nil {
		return nil, nil
	}
	u := &Usage{*r.Input, *r.Output, *r.Input + *r.Output}
	if !u.valid() {
		return nil, errProtocol
	}
	return u, nil
}
