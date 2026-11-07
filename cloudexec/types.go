// Package cloudexec implements AWS activities. Profiles are immutable worker
// configuration; requests reference them and cannot substitute resource IDs.
package cloudexec

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"go-scheduler/durable"
	"go-scheduler/executioncontract"
)

type Retention struct {
	OnSuccess     string `json:"on_success"`
	OnFailure     string `json:"on_failure"`
	RetainSeconds int    `json:"retain_seconds"`
}
type Compute struct {
	Mode               string     `json:"mode"`
	InstanceID         string     `json:"instance_id,omitempty"`
	ImageID            string     `json:"image_id,omitempty"`
	InstanceType       string     `json:"instance_type,omitempty"`
	SubnetID           string     `json:"subnet_id,omitempty"`
	SecurityGroups     []string   `json:"security_group_ids,omitempty"`
	InstanceProfileARN string     `json:"instance_profile_arn,omitempty"`
	Retention          *Retention `json:"retention,omitempty"`
}
type Volume struct {
	Mode      string    `json:"mode"`
	ID        string    `json:"volume_id,omitempty"`
	Mount     string    `json:"mount_path"`
	ReadOnly  bool      `json:"read_only"`
	Size      int32     `json:"size_gib,omitempty"`
	Type      string    `json:"volume_type,omitempty"`
	Encrypted bool      `json:"encrypted,omitempty"`
	KMS       string    `json:"kms_key_arn,omitempty"`
	Retention Retention `json:"retention"`
}
type Runtime struct {
	OS           string `json:"os"`
	Architecture string `json:"architecture"`
	ManifestHash string `json:"manifest_hash"`
	RunnerHash   string `json:"runner_hash"`
}
type S3Policy struct {
	Inputs     []string `json:"input_prefixes"`
	Output     string   `json:"output_prefix"`
	Encryption string   `json:"encryption"`
	KMS        string   `json:"kms_key_arn,omitempty"`
}
type Limits struct {
	Bootstrap   int `json:"bootstrap_seconds"`
	Job         int `json:"job_seconds"`
	Collection  int `json:"collection_seconds"`
	Concurrency int `json:"max_concurrent_jobs"`
}
type Profile struct {
	Schema   string   `json:"schema"`
	Account  string   `json:"account_id"`
	Region   string   `json:"region"`
	AuthMode string   `json:"controller_auth_mode,omitempty"`
	Role     string   `json:"controller_role_arn,omitempty"`
	Compute  Compute  `json:"compute"`
	Runtime  Runtime  `json:"runtime"`
	S3       S3Policy `json:"s3"`
	Volumes  []Volume `json:"volumes"`
	Limits   Limits   `json:"limits"`
}

// Request IDs are derived from the reviewed plan, not Temporal activity attempt.
type Request struct {
	ProfileConfigHash string `json:"profile_config_hash"`
	JobID             string `json:"job_id"`
	OwnerID           string `json:"owner_id"`
	ProfileKey        string `json:"profile_key"`
	NodeID            string `json:"node_id"`
	Attempt           int    `json:"attempt"`
	PlanHash          string `json:"plan_hash"`
}
type VolumeState struct {
	CreateIntent     bool   `json:"create_intent"`
	CleanupState     string `json:"cleanup_state,omitempty"`
	ID               string `json:"id"`
	Spec             Volume `json:"spec"`
	Owned            bool   `json:"owned"`
	AttachedByUs     bool   `json:"attached_by_us"`
	AttachmentIntent bool   `json:"attachment_intent"`
	Ready            bool   `json:"ready"`
}
type State struct {
	BootDeleteIntents  map[string]bool     `json:"boot_delete_intents,omitempty"`
	OwnedBootVolumes   []string            `json:"owned_boot_volumes,omitempty"`
	LaunchIntent       bool                `json:"launch_intent"`
	ActiveCollection   string              `json:"active_collection,omitempty"`
	TerminationIntent  bool                `json:"termination_intent"`
	MountsReleased     bool                `json:"mounts_released"`
	Dispatches         map[string]Dispatch `json:"dispatches"`
	Request            Request             `json:"request"`
	Profile            Profile             `json:"profile"`
	Fingerprint        string              `json:"fingerprint"`
	InstanceID         string              `json:"instance_id,omitempty"`
	OwnedInstance      bool                `json:"owned_instance"`
	AZ                 string              `json:"az,omitempty"`
	Volumes            []VolumeState       `json:"volumes"`
	SSMCommands        map[string]string   `json:"ssm_commands"`
	Phase              string              `json:"phase"`
	ProcessStopped     bool                `json:"process_stopped"`
	ComputeOutcome     string              `json:"compute_outcome"`
	PublicationOutcome string              `json:"publication_outcome"`
	CleanupOutcome     string              `json:"cleanup_outcome"`
	RetainUntil        string              `json:"retain_until,omitempty"`
	Errors             []string            `json:"errors,omitempty"`
}
type Probe struct {
	Ready       bool     `json:"ready"`
	TargetReady bool     `json:"target_ready"`
	AZ          string   `json:"availability_zone"`
	Blockers    []string `json:"blockers"`
}
type Controller struct {
	Profiles map[string]Profile
	Store    durable.Store
	Clients  func(context.Context, Profile) (EC2API, SSMAPI, STSAPI, error)
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func token(parts ...string) string {
	b, _ := json.Marshal(parts)
	return fmt.Sprintf("%x", sha256.Sum256(b))
}
func (p Profile) Validate() error {
	b, e := json.Marshal(p)
	if e != nil {
		return e
	}
	_, e = executioncontract.Validate(b)
	return e
}
func (c *Controller) profile(r Request) (Profile, error) {
	if !identifier.MatchString(r.JobID) || !identifier.MatchString(r.OwnerID) || !identifier.MatchString(r.NodeID) || r.Attempt < 1 || !regexp.MustCompile(`^sha256:[a-f0-9]{64}$`).MatchString(r.PlanHash) {
		return Profile{}, fmt.Errorf("invalid execution identity")
	}
	p, ok := c.Profiles[r.ProfileKey]
	if !ok {
		return Profile{}, fmt.Errorf("profile revision not served by worker")
	}
	if r.ProfileConfigHash == "" || r.ProfileConfigHash != ProfileHash(p) {
		return Profile{}, fmt.Errorf("worker profile differs from reviewed configuration")
	}
	return p, p.Validate()
}
func ProfileHash(p Profile) string {
	raw, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	v, err := executioncontract.Decode(raw)
	if err != nil {
		return ""
	}
	h, _ := executioncontract.CanonicalHash(v)
	return h
}
func stateKey(id string) string { return "aws-job/" + id }
func (c *Controller) Status(ctx context.Context, id string) (State, error) {
	var s State
	if !identifier.MatchString(id) {
		return s, fmt.Errorf("invalid job ID")
	}
	e := c.Store.Read(ctx, stateKey(id), &s)
	return s, e
}

type EC2API interface {
	DescribeInstances(context.Context, *ec2.DescribeInstancesInput, ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error)
	DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error)
	DescribeSubnets(context.Context, *ec2.DescribeSubnetsInput, ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error)
	DescribeSecurityGroups(context.Context, *ec2.DescribeSecurityGroupsInput, ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error)
	DescribeVolumes(context.Context, *ec2.DescribeVolumesInput, ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error)
	RunInstances(context.Context, *ec2.RunInstancesInput, ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error)
	CreateVolume(context.Context, *ec2.CreateVolumeInput, ...func(*ec2.Options)) (*ec2.CreateVolumeOutput, error)
	AttachVolume(context.Context, *ec2.AttachVolumeInput, ...func(*ec2.Options)) (*ec2.AttachVolumeOutput, error)
	DetachVolume(context.Context, *ec2.DetachVolumeInput, ...func(*ec2.Options)) (*ec2.DetachVolumeOutput, error)
	TerminateInstances(context.Context, *ec2.TerminateInstancesInput, ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error)
	DeleteVolume(context.Context, *ec2.DeleteVolumeInput, ...func(*ec2.Options)) (*ec2.DeleteVolumeOutput, error)
	ModifyInstanceAttribute(context.Context, *ec2.ModifyInstanceAttributeInput, ...func(*ec2.Options)) (*ec2.ModifyInstanceAttributeOutput, error)
}
type SSMAPI interface {
	DescribeInstanceInformation(context.Context, *ssm.DescribeInstanceInformationInput, ...func(*ssm.Options)) (*ssm.DescribeInstanceInformationOutput, error)
	SendCommand(context.Context, *ssm.SendCommandInput, ...func(*ssm.Options)) (*ssm.SendCommandOutput, error)
	GetCommandInvocation(context.Context, *ssm.GetCommandInvocationInput, ...func(*ssm.Options)) (*ssm.GetCommandInvocationOutput, error)
	ListCommands(context.Context, *ssm.ListCommandsInput, ...func(*ssm.Options)) (*ssm.ListCommandsOutput, error)
	CancelCommand(context.Context, *ssm.CancelCommandInput, ...func(*ssm.Options)) (*ssm.CancelCommandOutput, error)
}
type STSAPI interface {
	GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error)
}
