package cloudexec

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/smithy-go"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	et "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"go-scheduler/durable"
)

// Cleanup acts only after runner exit and unmount confirmation. Failure retains
// owned storage until its explicit deadline. Existing resources are never deleted.
func (c *Controller) Cleanup(ctx context.Context, id string) (State, error) {
	var result State
	err := c.Store.With(ctx, stateKey(id), func(entry *durable.Entry) error {
		var s State
		if e := entry.Read(&s); e != nil {
			return e
		}
		result = s
		if s.CleanupOutcome == "complete" {
			return nil
		}
		if !s.ProcessStopped {
			return fmt.Errorf("process exit is unconfirmed; preserve resources and leases")
		}
		if !s.MountsReleased {
			return fmt.Errorf("runner mount cleanup is unconfirmed")
		}
		successful := s.ComputeOutcome == "succeeded" && s.PublicationOutcome == "succeeded"
		retain := !successful
		for _, v := range s.Volumes {
			if v.Owned && v.Spec.Retention.OnSuccess == "retain" {
				retain = true
			}
		}
		retentionActive := false
		if retain {
			seconds := 0
			for _, v := range s.Volumes {
				if v.Spec.Retention.RetainSeconds > seconds {
					seconds = v.Spec.Retention.RetainSeconds
				}
			}
			if s.Profile.Compute.Retention != nil && s.Profile.Compute.Retention.RetainSeconds > seconds {
				seconds = s.Profile.Compute.Retention.RetainSeconds
			}
			if s.RetainUntil == "" {
				s.RetainUntil = time.Now().UTC().Add(time.Duration(seconds) * time.Second).Format(time.RFC3339)
				if e := entry.Write(s); e != nil {
					return e
				}
			}
			until, e := time.Parse(time.RFC3339, s.RetainUntil)
			if e != nil {
				return e
			}
			retentionActive = time.Now().Before(until)
			if retentionActive && !successful {
				s.CleanupOutcome = "retained"
				s.Phase = "terminal"
				result = s
				return entry.Write(s)
			}
		}
		api, _, _, e := c.Clients(ctx, s.Profile)
		if e != nil {
			return e
		}
		if e = c.recoverResourceReceipts(ctx, api, &s); e != nil {
			return e
		}
		if e = entry.Write(s); e != nil {
			return e
		}
		allDone := true
		storageRetained := false
		for index := range s.Volumes {
			v := &s.Volumes[index]
			if v.ID == "" {
				continue
			}
			if !v.Owned {
				held, err := c.holdsLease(ctx, s.Profile, v.ID, id)
				if err != nil {
					return err
				}
				if !held {
					continue
				}
			}
			if v.CleanupState == "deleted" {
				continue
			}
			actual, e := volume(ctx, api, v.ID)
			if e != nil {
				var apiErr smithy.APIError
				// An operator may already have deleted a volume that we previously
				// observed attached. Confirm its absence without stranding capacity.
				// An unobserved creation still requires our durable delete intent.
				if v.Owned && (v.CleanupState == "delete_intent" || v.Ready) && errors.As(e, &apiErr) && apiErr.ErrorCode() == "InvalidVolume.NotFound" {
					v.CleanupState = "deleted"
					if e = entry.Write(s); e != nil {
						return e
					}
					continue
				}
				return e
			}
			if v.Owned && !owned(actual.Tags, s.Request) {
				return fmt.Errorf("refusing cleanup of volume without exact ownership tags")
			}
			if len(actual.Attachments) > 0 {
				if v.Spec.Mode == "mounted" {
					if e = c.releaseLease(ctx, s.Profile, v.ID, id); e != nil {
						return e
					}
					continue
				}
				if !v.AttachedByUs && !v.AttachmentIntent {
					return fmt.Errorf("attachment ownership unknown")
				}
				for _, a := range actual.Attachments {
					if aws.ToString(a.InstanceId) != s.InstanceID {
						return fmt.Errorf("volume attachment changed")
					}
				}
				// Never force detach; a failed/busy unmount must remain actionable.
				if actual.Attachments[0].State != et.VolumeAttachmentStateDetaching {
					_, e = api.DetachVolume(ctx, &ec2.DetachVolumeInput{InstanceId: aws.String(s.InstanceID), VolumeId: aws.String(v.ID), Force: aws.Bool(false)})
					if e != nil {
						return e
					}
				}
				allDone = false
				continue
			}
			if v.Owned && successful && retentionActive && v.Spec.Retention.OnSuccess == "retain" {
				v.CleanupState = "retained"
				storageRetained = true
				continue
			}
			if v.Owned {
				if actual.State != et.VolumeStateDeleting {
					v.CleanupState = "delete_intent"
					if e = entry.Write(s); e != nil {
						return e
					}
					_, e = api.DeleteVolume(ctx, &ec2.DeleteVolumeInput{VolumeId: aws.String(v.ID)})
					if e != nil {
						return e
					}
				}
			}
			if v.CleanupState == "delete_intent" {
				allDone = false
			}
			if !v.Owned {
				if e = c.releaseLease(ctx, s.Profile, v.ID, id); e != nil {
					return e
				}
			}
		}
		if !allDone {
			s.CleanupOutcome = "pending"
			result = s
			return entry.Write(s)
		}
		if s.OwnedInstance && s.InstanceID != "" {
			i, e := instance(ctx, api, s.InstanceID)
			if e != nil {
				var ae smithy.APIError
				if !s.TerminationIntent || !errors.As(e, &ae) || ae.ErrorCode() != "InvalidInstanceID.NotFound" {
					return e
				}
				i = et.Instance{State: &et.InstanceState{Name: et.InstanceStateNameTerminated}, Tags: tags(s.Request)}
			}
			if !owned(i.Tags, s.Request) {
				return fmt.Errorf("refusing cleanup of instance without exact ownership tags")
			}
			if i.State == nil || i.State.Name != et.InstanceStateNameTerminated {
				if i.State == nil || i.State.Name != et.InstanceStateNameShuttingDown {
					// Do not let DeleteOnTermination affect a volume attached by somebody
					// else while the job ran. Unknown attachments block termination.
					for _, mapping := range i.BlockDeviceMappings {
						if mapping.Ebs == nil {
							continue
						}
						volumeID := aws.ToString(mapping.Ebs.VolumeId)
						if volumeID == "" {
							continue
						}
						knownWork := false
						for _, v := range s.Volumes {
							if v.ID == volumeID {
								knownWork = true
								break
							}
						}
						if knownWork {
							continue
						}
						boot, err := volume(ctx, api, volumeID)
						if err != nil {
							return err
						}
						if !owned(boot.Tags, s.Request) {
							return fmt.Errorf("unexpected unowned attachment blocks managed instance termination")
						}
						knownBoot := false
						for _, id := range s.OwnedBootVolumes {
							if id == volumeID {
								knownBoot = true
								break
							}
						}
						if !knownBoot {
							s.OwnedBootVolumes = append(s.OwnedBootVolumes, volumeID)
						}
					}
					s.TerminationIntent = true
					if e = entry.Write(s); e != nil {
						return e
					}
					_, e = api.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{s.InstanceID}})
					if e != nil {
						return e
					}
				}
				s.CleanupOutcome = "pending"
				result = s
				return entry.Write(s)
			}
		} else if s.InstanceID != "" {
			held, err := c.holdsLease(ctx, s.Profile, s.InstanceID, id)
			if err != nil {
				return err
			}
			if held {
				if e = c.releaseLease(ctx, s.Profile, s.InstanceID, id); e != nil {
					return e
				}
			}
		}
		// Instance termination is not itself evidence that all AMI-created EBS
		// volumes disappeared. Confirm their disposition using the recorded IDs.
		for _, bootID := range s.OwnedBootVolumes {
			actual, err := volume(ctx, api, bootID)
			if err != nil {
				var ae smithy.APIError
				if errors.As(err, &ae) && ae.ErrorCode() == "InvalidVolume.NotFound" {
					continue
				}
				return err
			}
			if !owned(actual.Tags, s.Request) || len(actual.Attachments) > 0 {
				return fmt.Errorf("managed boot volume cleanup is unconfirmed")
			}
			if actual.State != et.VolumeStateDeleting {
				if s.BootDeleteIntents == nil {
					s.BootDeleteIntents = map[string]bool{}
				}
				s.BootDeleteIntents[bootID] = true
				if err = entry.Write(s); err != nil {
					return err
				}
				if _, err = api.DeleteVolume(ctx, &ec2.DeleteVolumeInput{VolumeId: aws.String(bootID)}); err != nil {
					return err
				}
			}
			s.CleanupOutcome = "pending"
			result = s
			return entry.Write(s)
		}
		s.CleanupOutcome = "complete"
		if storageRetained {
			s.CleanupOutcome = "retained"
		}
		if _, e = c.capacity(ctx, s.Request, s.Profile, true); e != nil {
			return e
		}
		s.Phase = "terminal"
		result = s
		return entry.Write(s)
	})
	return result, err
}
