package cloudexec

import (
	"context"
	"go-scheduler/durable"
	"os"
)

// Slots are durable, shared across workflows and worker replicas. They are
// released only after confirmed resource cleanup, never on elapsed lease time.
func (c *Controller) capacity(ctx context.Context, r Request, p Profile, release bool) (bool, error) {
	acquired := false
	err := c.Store.With(ctx, "capacity/"+p.Account+"/"+p.Region+"/"+r.ProfileKey, func(e *durable.Entry) error {
		owners := map[string]bool{}
		if err := e.Read(&owners); err != nil && !os.IsNotExist(err) {
			return err
		}
		if release {
			delete(owners, r.JobID)
		} else {
			if !owners[r.JobID] && len(owners) >= p.Limits.Concurrency {
				return nil
			}
			owners[r.JobID] = true
			acquired = true
		}
		return e.Write(owners)
	})
	return acquired, err
}
