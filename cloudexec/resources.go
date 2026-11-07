package cloudexec

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	et "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	st "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"go-scheduler/durable"
)

func instance(ctx context.Context, api EC2API, id string) (et.Instance, error) {
	out, e := api.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{id}})
	if e != nil {
		return et.Instance{}, e
	}
	for _, r := range out.Reservations {
		for _, i := range r.Instances {
			if aws.ToString(i.InstanceId) == id {
				return i, nil
			}
		}
	}
	return et.Instance{}, &smithy.GenericAPIError{Code: "InvalidInstanceID.NotFound", Message: "instance not found"}
}
func volume(ctx context.Context, api EC2API, id string) (et.Volume, error) {
	out, e := api.DescribeVolumes(ctx, &ec2.DescribeVolumesInput{VolumeIds: []string{id}})
	if e != nil {
		return et.Volume{}, e
	}
	if len(out.Volumes) != 1 {
		return et.Volume{}, fmt.Errorf("volume not found")
	}
	return out.Volumes[0], nil
}
func ssmReady(ctx context.Context, api SSMAPI, id string) (bool, error) {
	out, e := api.DescribeInstanceInformation(ctx, &ssm.DescribeInstanceInformationInput{Filters: []st.InstanceInformationStringFilter{{Key: aws.String("InstanceIds"), Values: []string{id}}}})
	if e != nil {
		return false, e
	}
	for _, i := range out.InstanceInformationList {
		if aws.ToString(i.InstanceId) == id && i.PingStatus == st.PingStatusOnline && i.PlatformType == st.PlatformTypeLinux {
			return true, nil
		}
	}
	return false, nil
}
func tags(r Request) []et.Tag {
	return []et.Tag{{Key: aws.String("workbench:job"), Value: aws.String(r.JobID)}, {Key: aws.String("workbench:identity"), Value: aws.String(token(r.OwnerID, r.ProfileKey, r.NodeID, fmt.Sprint(r.Attempt), r.PlanHash))}}
}
func owned(actual []et.Tag, r Request) bool {
	want := tags(r)
	for _, w := range want {
		found := false
		for _, a := range actual {
			if aws.ToString(a.Key) == aws.ToString(w.Key) && aws.ToString(a.Value) == aws.ToString(w.Value) {
				found = true
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// Probe performs describe/get operations only; managed target readiness is false
// until provisioning and the runner validate the actual disk/runtime.
func (c *Controller) Probe(ctx context.Context, r Request) (Probe, error) {
	p, e := c.profile(r)
	if e != nil {
		return Probe{}, e
	}
	api, ssmAPI, identity, e := c.Clients(ctx, p)
	if e != nil {
		return Probe{}, e
	}
	result := Probe{Blockers: []string{}}
	caller, e := identity.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if e != nil {
		return result, e
	}
	if aws.ToString(caller.Account) != p.Account {
		result.Blockers = append(result.Blockers, "AWS caller account does not match profile")
	}
	if p.Compute.Mode == "managed" {
		images, e := api.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{p.Compute.ImageID}})
		if e != nil {
			return result, e
		}
		if len(images.Images) != 1 || string(images.Images[0].Architecture) != p.Runtime.Architecture || images.Images[0].State != et.ImageStateAvailable {
			result.Blockers = append(result.Blockers, "AMI unavailable or architecture mismatch")
		}
		subnets, e := api.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: []string{p.Compute.SubnetID}})
		if e != nil {
			return result, e
		}
		if len(subnets.Subnets) != 1 {
			return result, fmt.Errorf("subnet not found")
		}
		subnet := subnets.Subnets[0]
		result.AZ = aws.ToString(subnet.AvailabilityZone)
		groups, e := api.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: p.Compute.SecurityGroups})
		if e != nil {
			return result, e
		}
		if len(groups.SecurityGroups) != len(p.Compute.SecurityGroups) {
			result.Blockers = append(result.Blockers, "missing security groups")
		}
		for _, g := range groups.SecurityGroups {
			if aws.ToString(g.VpcId) != aws.ToString(subnet.VpcId) {
				result.Blockers = append(result.Blockers, "security group VPC mismatch")
			}
		}
	} else {
		i, e := instance(ctx, api, p.Compute.InstanceID)
		if e != nil {
			return result, e
		}
		result.AZ = aws.ToString(i.Placement.AvailabilityZone)
		if i.State == nil || i.State.Name != et.InstanceStateNameRunning || string(i.Architecture) != p.Runtime.Architecture {
			result.Blockers = append(result.Blockers, "existing instance is not running with the pinned architecture")
		}
		ready, e := ssmReady(ctx, ssmAPI, p.Compute.InstanceID)
		if e != nil {
			return result, e
		}
		if !ready {
			result.Blockers = append(result.Blockers, "SSM agent is not online")
		}
	}
	for _, spec := range p.Volumes {
		if spec.Mode == "new" {
			continue
		}
		v, e := volume(ctx, api, spec.ID)
		if e != nil {
			return result, e
		}
		if aws.ToString(v.AvailabilityZone) != result.AZ {
			result.Blockers = append(result.Blockers, "volume is in a different Availability Zone")
		}
		if !aws.ToBool(v.Encrypted) {
			result.Blockers = append(result.Blockers, "existing volume must be encrypted")
		}
		if spec.Mode == "existing" && len(v.Attachments) != 0 {
			result.Blockers = append(result.Blockers, "existing volume is attached")
		}
		if spec.Mode == "mounted" {
			found := false
			for _, a := range v.Attachments {
				if aws.ToString(a.InstanceId) == p.Compute.InstanceID {
					found = true
				}
			}
			if !found {
				result.Blockers = append(result.Blockers, "mounted volume is not attached to the existing instance")
			}
		}
	}
	result.Ready = len(result.Blockers) == 0
	return result, nil
}

type lease struct {
	Owner    string `json:"owner"`
	Released bool   `json:"released"`
}

func (c *Controller) acquireLease(ctx context.Context, p Profile, resource, owner string) error {
	return c.Store.With(ctx, "lease/"+p.Account+"/"+p.Region+"/"+resource, func(e *durable.Entry) error {
		var l lease
		err := e.Read(&l)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if l.Owner != "" && !l.Released && l.Owner != owner {
			return fmt.Errorf("resource is leased to another job; reconciliation is required")
		}
		return e.Write(lease{Owner: owner})
	})
}
func (c *Controller) releaseLease(ctx context.Context, p Profile, resource, owner string) error {
	return c.Store.With(ctx, "lease/"+p.Account+"/"+p.Region+"/"+resource, func(e *durable.Entry) error {
		var l lease
		if err := e.Read(&l); err != nil {
			return err
		}
		if l.Owner != owner {
			return fmt.Errorf("lease owner mismatch")
		}
		l.Released = true
		return e.Write(l)
	})
}
func (c *Controller) Acquire(ctx context.Context, r Request) (State, error) {
	p, e := c.profile(r)
	if e != nil {
		return State{}, e
	}
	var result State
	e = c.Store.With(ctx, stateKey(r.JobID), func(entry *durable.Entry) error {
		var s State
		raw, _ := json.Marshal(struct {
			Request Request
			Profile Profile
		}{r, p})
		fingerprint := token(string(raw))
		if e := entry.Read(&s); e == nil {
			if s.Fingerprint != fingerprint {
				return fmt.Errorf("AWS job identity conflicts with persisted intent")
			}
		} else if !os.IsNotExist(e) {
			return e
		} else {
			s = State{Request: r, Profile: p, Fingerprint: fingerprint, Phase: "acquiring_compute", ProcessStopped: true, MountsReleased: true, SSMCommands: map[string]string{}}
			for _, v := range p.Volumes {
				s.Volumes = append(s.Volumes, VolumeState{Spec: v})
			}
			if e = entry.Write(s); e != nil {
				return e
			}
		}
		reserved, e := c.capacity(ctx, r, p, false)
		if e != nil {
			return e
		}
		if !reserved {
			s.Phase = "waiting_capacity"
			result = s
			return entry.Write(s)
		}
		api, _, _, e := c.Clients(ctx, p)
		if e != nil {
			return e
		}
		if p.Compute.Mode == "existing" {
			s.InstanceID = p.Compute.InstanceID
			if e = entry.Write(s); e != nil {
				return e
			}
			if e = c.acquireLease(ctx, p, p.Compute.InstanceID, r.JobID); e != nil {
				return e
			}
			s.InstanceID = p.Compute.InstanceID
		} else if s.InstanceID == "" {
			// Ensure all AMI-created EBS storage is deleted with our managed instance.
			images, err := api.DescribeImages(ctx, &ec2.DescribeImagesInput{ImageIds: []string{p.Compute.ImageID}})
			if err != nil {
				return err
			}
			if len(images.Images) != 1 {
				return fmt.Errorf("AMI not found")
			}
			mappings := []et.BlockDeviceMapping{}
			for _, mapping := range images.Images[0].BlockDeviceMappings {
				if mapping.Ebs != nil {
					mappings = append(mappings, et.BlockDeviceMapping{DeviceName: mapping.DeviceName, Ebs: &et.EbsBlockDevice{DeleteOnTermination: aws.Bool(true), Encrypted: aws.Bool(true)}})
				}
			}
			s.LaunchIntent = true
			if e = entry.Write(s); e != nil {
				return e
			}
			// EC2 client tokens recover accepted-but-unrecorded RunInstances calls.
			out, e := api.RunInstances(ctx, &ec2.RunInstancesInput{ClientToken: aws.String(token(r.JobID, "instance")), BlockDeviceMappings: mappings, ImageId: aws.String(p.Compute.ImageID), InstanceType: et.InstanceType(p.Compute.InstanceType), SubnetId: aws.String(p.Compute.SubnetID), SecurityGroupIds: p.Compute.SecurityGroups, IamInstanceProfile: &et.IamInstanceProfileSpecification{Arn: aws.String(p.Compute.InstanceProfileARN)}, MinCount: aws.Int32(1), MaxCount: aws.Int32(1), MetadataOptions: &et.InstanceMetadataOptionsRequest{HttpTokens: et.HttpTokensStateRequired}, TagSpecifications: []et.TagSpecification{{ResourceType: et.ResourceTypeInstance, Tags: tags(r)}, {ResourceType: et.ResourceTypeVolume, Tags: tags(r)}}})
			if e != nil {
				return e
			}
			if len(out.Instances) != 1 {
				return fmt.Errorf("unexpected launch result")
			}
			s.InstanceID = aws.ToString(out.Instances[0].InstanceId)
			s.OwnedInstance = true
		}
		if e = entry.Write(s); e != nil {
			return e
		}
		i, e := instance(ctx, api, s.InstanceID)
		if e != nil {
			return e
		}
		if s.OwnedInstance && !owned(i.Tags, r) {
			return fmt.Errorf("instance ownership mismatch")
		}
		if s.OwnedInstance && len(s.OwnedBootVolumes) == 0 {
			for _, mapping := range i.BlockDeviceMappings {
				if mapping.Ebs != nil && aws.ToString(mapping.Ebs.VolumeId) != "" {
					s.OwnedBootVolumes = append(s.OwnedBootVolumes, aws.ToString(mapping.Ebs.VolumeId))
				}
			}
		}
		s.AZ = aws.ToString(i.Placement.AvailabilityZone)
		if i.State == nil || i.State.Name != et.InstanceStateNameRunning {
			s.Phase = "acquiring_compute"
		} else {
			s.Phase = "preparing_storage"
		}
		result = s
		return entry.Write(s)
	})
	return result, e
}
func (c *Controller) PrepareStorage(ctx context.Context, id string) (State, error) {
	var result State
	err := c.Store.With(ctx, stateKey(id), func(entry *durable.Entry) error {
		var s State
		if e := entry.Read(&s); e != nil {
			return e
		}
		if s.InstanceID == "" || s.AZ == "" {
			return fmt.Errorf("compute not acquired")
		}
		api, _, _, e := c.Clients(ctx, s.Profile)
		if e != nil {
			return e
		}
		allReady := true
		for index := range s.Volumes {
			v := &s.Volumes[index]
			spec := v.Spec
			if v.ID == "" {
				if spec.Mode == "new" {
					v.CreateIntent = true
					if e = entry.Write(s); e != nil {
						return e
					}
					volumeTags := append(tags(s.Request), et.Tag{Key: aws.String("workbench:volume-index"), Value: aws.String(fmt.Sprint(index))})
					out, e := api.CreateVolume(ctx, &ec2.CreateVolumeInput{ClientToken: aws.String(token(id, "volume", fmt.Sprint(index))), AvailabilityZone: aws.String(s.AZ), Encrypted: aws.Bool(true), Size: aws.Int32(spec.Size), VolumeType: et.VolumeTypeGp3, KmsKeyId: optional(spec.KMS), TagSpecifications: []et.TagSpecification{{ResourceType: et.ResourceTypeVolume, Tags: volumeTags}}})
					if e != nil {
						return e
					}
					v.ID = aws.ToString(out.VolumeId)
					v.Owned = true
				} else {
					v.ID = spec.ID
					if e = entry.Write(s); e != nil {
						return e
					}
					if e = c.acquireLease(ctx, s.Profile, spec.ID, id); e != nil {
						return e
					}
					v.ID = spec.ID
				}
				if e = entry.Write(s); e != nil {
					return e
				}
			}
			if !v.Owned {
				if e = c.acquireLease(ctx, s.Profile, v.ID, id); e != nil {
					return e
				}
			}
			actual, e := volume(ctx, api, v.ID)
			if e != nil {
				return e
			}
			if aws.ToString(actual.AvailabilityZone) != s.AZ {
				return fmt.Errorf("volume AZ mismatch")
			}
			if v.Owned && !owned(actual.Tags, s.Request) {
				return fmt.Errorf("volume ownership mismatch")
			}
			attached := false
			for _, a := range actual.Attachments {
				if aws.ToString(a.InstanceId) != s.InstanceID {
					return fmt.Errorf("volume attached to another instance")
				}
				if a.State == et.VolumeAttachmentStateAttached {
					attached = true
				}
			}
			if spec.Mode == "mounted" {
				if !attached {
					return fmt.Errorf("pre-existing mount volume is not attached")
				}
				v.Ready = true
				continue
			}
			if attached {
				if !v.AttachmentIntent && !v.AttachedByUs {
					return fmt.Errorf("unexpected pre-existing attachment")
				}
				v.AttachedByUs = true
				// Preserve volumes on instance termination; explicit owned-volume cleanup
				// occurs only after publication and confirmed process exit.
				inst, e := instance(ctx, api, s.InstanceID)
				if e != nil {
					return e
				}
				device := ""
				for _, b := range inst.BlockDeviceMappings {
					if b.Ebs != nil && aws.ToString(b.Ebs.VolumeId) == v.ID {
						device = aws.ToString(b.DeviceName)
					}
				}
				if device == "" {
					allReady = false
					continue
				}
				_, e = api.ModifyInstanceAttribute(ctx, &ec2.ModifyInstanceAttributeInput{InstanceId: aws.String(s.InstanceID), BlockDeviceMappings: []et.InstanceBlockDeviceMappingSpecification{{DeviceName: aws.String(device), Ebs: &et.EbsInstanceBlockDeviceSpecification{DeleteOnTermination: aws.Bool(false)}}}})
				if e != nil {
					return e
				}
				v.Ready = true
			} else if actual.State == et.VolumeStateAvailable {
				v.AttachmentIntent = true
				if e = entry.Write(s); e != nil {
					return e
				}
				// The runner resolves NVMe identity by volume ID; this is an EC2 API alias.
				device := fmt.Sprintf("/dev/sd%c", 'f'+index)
				_, e = api.AttachVolume(ctx, &ec2.AttachVolumeInput{InstanceId: aws.String(s.InstanceID), VolumeId: aws.String(v.ID), Device: aws.String(device)})
				if e != nil {
					return e
				}
				allReady = false
			} else {
				allReady = false
			}
			if e = entry.Write(s); e != nil {
				return e
			}
		}
		if allReady {
			s.Phase = "staging"
		}
		result = s
		return entry.Write(s)
	})
	return result, err
}
func optional(s string) *string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return aws.String(s)
}

func (c *Controller) holdsLease(ctx context.Context, p Profile, resource, owner string) (bool, error) {
	var l lease
	err := c.Store.Read(ctx, "lease/"+p.Account+"/"+p.Region+"/"+resource, &l)
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil && l.Owner == owner && !l.Released, err
}
