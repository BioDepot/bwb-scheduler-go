package cloudexec

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	et "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"go.temporal.io/sdk/temporal"
	"sort"
	"time"
)

// Read-only recovery of accepted-but-unrecorded resource creation. Absence is
// ambiguous under eventual consistency; it never authorizes releasing a lease.
func (c *Controller) recoverResourceReceipts(ctx context.Context, api EC2API, s *State) error {
	if s.LaunchIntent && s.InstanceID == "" {
		out, e := api.DescribeInstances(ctx, &ec2.DescribeInstancesInput{Filters: []et.Filter{{Name: aws.String("client-token"), Values: []string{token(s.Request.JobID, "instance")}}}})
		if e != nil {
			return e
		}
		matches := []et.Instance{}
		for _, reservation := range out.Reservations {
			for _, i := range reservation.Instances {
				if owned(i.Tags, s.Request) {
					matches = append(matches, i)
				}
			}
		}
		if len(matches) != 1 {
			return fmt.Errorf("launch receipt recovery is ambiguous (%d exact tagged resources); preserve capacity", len(matches))
		}
		s.InstanceID = aws.ToString(matches[0].InstanceId)
		s.OwnedInstance = true
		for _, mapping := range matches[0].BlockDeviceMappings {
			if mapping.Ebs != nil && aws.ToString(mapping.Ebs.VolumeId) != "" {
				s.OwnedBootVolumes = append(s.OwnedBootVolumes, aws.ToString(mapping.Ebs.VolumeId))
			}
		}
	}
	for index := range s.Volumes {
		v := &s.Volumes[index]
		if !v.CreateIntent || v.ID != "" {
			continue
		}
		filters := []et.Filter{{Name: aws.String("tag:workbench:volume-index"), Values: []string{fmt.Sprint(index)}}}
		for _, tag := range tags(s.Request) {
			filters = append(filters, et.Filter{Name: aws.String("tag:" + aws.ToString(tag.Key)), Values: []string{aws.ToString(tag.Value)}})
		}
		out, e := api.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{Filters: filters})
		if e != nil {
			return e
		}
		if len(out.Volumes) != 1 || !owned(out.Volumes[0].Tags, s.Request) {
			return fmt.Errorf("volume creation receipt is ambiguous; retain recovery intent")
		}
		v.ID = aws.ToString(out.Volumes[0].VolumeId)
		v.Owned = true
	}
	return nil
}

// Reconcile is invoked by the owner-scoped control API only after it verifies
// that orchestration has stopped. It never treats elapsed time as process exit.
func (c *Controller) Reconcile(ctx context.Context, id string, apply bool) (State, error) {
	state, e := c.Status(ctx, id)
	if e != nil {
		return state, e
	}
	if !apply {
		return state, nil
	}
	requests := map[string]RunnerRequest{}
	var mountRequest *RunnerRequest
	for _, d := range state.Dispatches {
		if d.Request == nil {
			continue
		}
		r := *d.Request
		if d.Action == "prepare" {
			mountRequest = &r
		}
		if d.Action == "start" {
			requests[r.CommandID] = r
		}
	}
	ids := []string{}
	for key := range requests {
		ids = append(ids, key)
	}
	sort.Strings(ids)
	nonce := fmt.Sprint(time.Now().UnixNano())
	invoke := func(r RunnerRequest, action string) (RunnerResult, error) {
		r.Action = action
		r.OperationID = token(id, nonce, r.CommandID, action)
		if _, e := c.DispatchRunner(ctx, id, r); e != nil {
			return RunnerResult{}, e
		}
		for {
			result, e := c.PollRunner(ctx, id, r.OperationID)
			if e != nil {
				var pending *temporal.ApplicationError
				if !errors.As(e, &pending) || pending.Type() != "NotReady" {
					return result, e
				}
			}
			if result.DeliveryState == "Success" {
				return result, nil
			}
			if result.DeliveryState == "Failed" || result.DeliveryState == "TimedOut" || result.DeliveryState == "Cancelled" {
				return result, fmt.Errorf("runner reconciliation delivery %s", result.DeliveryState)
			}
			select {
			case <-ctx.Done():
				return result, ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
	for _, key := range ids {
		r := requests[key]
		result, e := invoke(r, "status")
		if e != nil {
			return state, e
		}
		if !result.ProcessStopped || result.ProcessState == "not_started" {
			if result.ProcessState == "unknown" {
				return state, fmt.Errorf("command %s outcome unknown; preserve resources", key)
			}
			result, e = invoke(r, "cancel")
			if e != nil {
				return state, e
			}
			if !result.ProcessStopped || result.ProcessState == "not_started" {
				return state, fmt.Errorf("command %s cancellation requested; retry reconciliation after exit", key)
			}
		}
	}
	if len(ids) > 0 || state.ProcessStopped {
		if e = c.RecordOutcome(ctx, id, state.ComputeOutcome, state.PublicationOutcome, true); e != nil {
			return state, e
		}
	}
	if !state.MountsReleased {
		if mountRequest == nil {
			return state, fmt.Errorf("mount ownership request missing")
		}
		result, e := invoke(*mountRequest, "unmount")
		if e != nil || result.ProcessState != "unmounted" {
			return state, fmt.Errorf("mount cleanup unconfirmed: %v", e)
		}
	}
	return c.Cleanup(ctx, id)
}
