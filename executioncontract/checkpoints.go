package executioncontract

import "fmt"

func checkpointsCheck(p Document, nodes map[string]Document) error {
	raw, ok := p["checkpoints"]
	if !ok {
		return nil
	}
	seen := map[string]bool{}
	for _, item := range list(raw) {
		gate := object(item)
		id := str(gate["checkpoint_id"])
		if seen[id] {
			return fmt.Errorf("duplicate checkpoint ID")
		}
		seen[id] = true
		for _, v := range append(append([]any{}, list(gate["after_nodes"])...), list(gate["before_nodes"])...) {
			if nodes[str(v)] == nil {
				return fmt.Errorf("checkpoint references unknown node")
			}
		}
		for _, v := range list(gate["before_nodes"]) {
			ancestors := map[string]bool{}
			pending := append([]any{}, list(nodes[str(v)]["depends_on"])...)
			for len(pending) > 0 {
				id := str(pending[0])
				pending = pending[1:]
				if !ancestors[id] {
					ancestors[id] = true
					pending = append(pending, list(nodes[id]["depends_on"])...)
				}
			}
			for _, after := range list(gate["after_nodes"]) {
				if !ancestors[str(after)] {
					return fmt.Errorf("checkpoint milestones must be ancestors of every blocked node")
				}
			}
		}
	}
	return nil
}
