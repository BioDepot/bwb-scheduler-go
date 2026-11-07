package cloudexec

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	et "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	st "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"go-scheduler/durable"
)

type fakeEC2 struct {
	EC2API
	instances                                                  map[string]et.Instance
	volumes                                                    map[string]et.Volume
	tokens                                                     map[string]string
	launches, creates, attaches, modifies, terminates, deletes int
	lostLaunch                                                 bool
	lostCreate                                                 bool
	az                                                         string
}

func newEC2() *fakeEC2 {
	return &fakeEC2{instances: map[string]et.Instance{}, volumes: map[string]et.Volume{}, tokens: map[string]string{}, az: "us-west-2a"}
}
func (f *fakeEC2) DescribeInstances(_ context.Context, in *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	out := &ec2.DescribeInstancesOutput{}
	for _, id := range in.InstanceIds {
		if i, ok := f.instances[id]; ok {
			out.Reservations = append(out.Reservations, et.Reservation{Instances: []et.Instance{i}})
		}
	}
	return out, nil
}
func (f *fakeEC2) DescribeImages(context.Context, *ec2.DescribeImagesInput, ...func(*ec2.Options)) (*ec2.DescribeImagesOutput, error) {
	return &ec2.DescribeImagesOutput{Images: []et.Image{{Architecture: et.ArchitectureValuesX8664, State: et.ImageStateAvailable, BlockDeviceMappings: []et.BlockDeviceMapping{{DeviceName: aws.String("/dev/sda1"), Ebs: &et.EbsBlockDevice{}}}}}}, nil
}
func (f *fakeEC2) DescribeSubnets(context.Context, *ec2.DescribeSubnetsInput, ...func(*ec2.Options)) (*ec2.DescribeSubnetsOutput, error) {
	return &ec2.DescribeSubnetsOutput{Subnets: []et.Subnet{{AvailabilityZone: aws.String(f.az), VpcId: aws.String("vpc-1")}}}, nil
}
func (f *fakeEC2) DescribeSecurityGroups(context.Context, *ec2.DescribeSecurityGroupsInput, ...func(*ec2.Options)) (*ec2.DescribeSecurityGroupsOutput, error) {
	return &ec2.DescribeSecurityGroupsOutput{SecurityGroups: []et.SecurityGroup{{VpcId: aws.String("vpc-1")}}}, nil
}
func (f *fakeEC2) DescribeVolumes(_ context.Context, in *ec2.DescribeVolumesInput, _ ...func(*ec2.Options)) (*ec2.DescribeVolumesOutput, error) {
	out := &ec2.DescribeVolumesOutput{}
	for _, id := range in.VolumeIds {
		v, ok := f.volumes[id]
		if !ok {
			return nil, &smithy.GenericAPIError{Code: "InvalidVolume.NotFound"}
		}
		out.Volumes = append(out.Volumes, v)
	}
	return out, nil
}
func (f *fakeEC2) RunInstances(_ context.Context, in *ec2.RunInstancesInput, _ ...func(*ec2.Options)) (*ec2.RunInstancesOutput, error) {
	id, ok := f.tokens[*in.ClientToken]
	if !ok {
		f.launches++
		id = "i-0123456789abcdef0"
		f.tokens[*in.ClientToken] = id
		f.instances[id] = et.Instance{InstanceId: aws.String(id), Architecture: et.ArchitectureValuesX8664, State: &et.InstanceState{Name: et.InstanceStateNameRunning}, Placement: &et.Placement{AvailabilityZone: aws.String(f.az)}, Tags: in.TagSpecifications[0].Tags}
	}
	if f.lostLaunch {
		f.lostLaunch = false
		return nil, fmt.Errorf("lost launch response")
	}
	return &ec2.RunInstancesOutput{Instances: []et.Instance{f.instances[id]}}, nil
}
func (f *fakeEC2) CreateVolume(_ context.Context, in *ec2.CreateVolumeInput, _ ...func(*ec2.Options)) (*ec2.CreateVolumeOutput, error) {
	id, ok := f.tokens[*in.ClientToken]
	if !ok {
		f.creates++
		id = "vol-0123456789abcdef0"
		f.tokens[*in.ClientToken] = id
		f.volumes[id] = et.Volume{VolumeId: aws.String(id), State: et.VolumeStateAvailable, AvailabilityZone: in.AvailabilityZone, Encrypted: aws.Bool(true), Tags: in.TagSpecifications[0].Tags}
	}
	if f.lostCreate {
		f.lostCreate = false
		return nil, fmt.Errorf("lost volume response")
	}
	return &ec2.CreateVolumeOutput{VolumeId: aws.String(id)}, nil
}
func (f *fakeEC2) AttachVolume(_ context.Context, in *ec2.AttachVolumeInput, _ ...func(*ec2.Options)) (*ec2.AttachVolumeOutput, error) {
	f.attaches++
	v := f.volumes[*in.VolumeId]
	v.Attachments = []et.VolumeAttachment{{InstanceId: in.InstanceId, State: et.VolumeAttachmentStateAttached}}
	v.State = et.VolumeStateInUse
	f.volumes[*in.VolumeId] = v
	i := f.instances[*in.InstanceId]
	i.BlockDeviceMappings = append(i.BlockDeviceMappings, et.InstanceBlockDeviceMapping{DeviceName: in.Device, Ebs: &et.EbsInstanceBlockDevice{VolumeId: in.VolumeId}})
	f.instances[*in.InstanceId] = i
	return &ec2.AttachVolumeOutput{}, nil
}
func (f *fakeEC2) ModifyInstanceAttribute(context.Context, *ec2.ModifyInstanceAttributeInput, ...func(*ec2.Options)) (*ec2.ModifyInstanceAttributeOutput, error) {
	f.modifies++
	return &ec2.ModifyInstanceAttributeOutput{}, nil
}
func (f *fakeEC2) DetachVolume(_ context.Context, in *ec2.DetachVolumeInput, _ ...func(*ec2.Options)) (*ec2.DetachVolumeOutput, error) {
	if aws.ToBool(in.Force) {
		panic("force detach")
	}
	v := f.volumes[*in.VolumeId]
	v.Attachments = nil
	v.State = et.VolumeStateAvailable
	f.volumes[*in.VolumeId] = v
	return &ec2.DetachVolumeOutput{}, nil
}
func (f *fakeEC2) TerminateInstances(_ context.Context, in *ec2.TerminateInstancesInput, _ ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	f.terminates++
	for _, id := range in.InstanceIds {
		i := f.instances[id]
		i.State.Name = et.InstanceStateNameTerminated
		f.instances[id] = i
	}
	return &ec2.TerminateInstancesOutput{}, nil
}
func (f *fakeEC2) DeleteVolume(_ context.Context, in *ec2.DeleteVolumeInput, _ ...func(*ec2.Options)) (*ec2.DeleteVolumeOutput, error) {
	f.deletes++
	delete(f.volumes, *in.VolumeId)
	return &ec2.DeleteVolumeOutput{}, nil
}

type fakeIdentity struct{ STSAPI }

func (fakeIdentity) GetCallerIdentity(context.Context, *sts.GetCallerIdentityInput, ...func(*sts.Options)) (*sts.GetCallerIdentityOutput, error) {
	return &sts.GetCallerIdentityOutput{Account: aws.String("123456789012")}, nil
}

type fakeSSM struct{ SSMAPI }

func (fakeSSM) DescribeInstanceInformation(_ context.Context, in *ssm.DescribeInstanceInformationInput, _ ...func(*ssm.Options)) (*ssm.DescribeInstanceInformationOutput, error) {
	return &ssm.DescribeInstanceInformationOutput{InstanceInformationList: []st.InstanceInformation{{InstanceId: aws.String(in.Filters[0].Values[0]), PingStatus: st.PingStatusOnline, PlatformType: st.PlatformTypeLinux}}}, nil
}
func fixture(t *testing.T, name string) (*Controller, *fakeEC2, Request) {
	t.Helper()
	b, e := os.ReadFile("../executioncontract/testdata/contracts-v1.json")
	if e != nil {
		t.Fatal(e)
	}
	var corpus struct {
		Valid map[string]json.RawMessage `json:"valid"`
	}
	if e = json.Unmarshal(b, &corpus); e != nil {
		t.Fatal(e)
	}
	var p Profile
	if e = json.Unmarshal(corpus.Valid[name], &p); e != nil {
		t.Fatal(e)
	}
	if e = p.Validate(); e != nil {
		t.Fatal(e)
	}
	f := newEC2()
	if p.Compute.Mode == "existing" {
		f.instances[p.Compute.InstanceID] = et.Instance{InstanceId: aws.String(p.Compute.InstanceID), State: &et.InstanceState{Name: et.InstanceStateNameRunning}, Architecture: et.ArchitectureValuesX8664, Placement: &et.Placement{AvailabilityZone: aws.String(f.az)}}
		for _, v := range p.Volumes {
			f.volumes[v.ID] = et.Volume{VolumeId: aws.String(v.ID), AvailabilityZone: aws.String(f.az), Encrypted: aws.Bool(true), State: et.VolumeStateAvailable}
			if v.Mode == "mounted" {
				actual := f.volumes[v.ID]
				actual.Attachments = []et.VolumeAttachment{{InstanceId: aws.String(p.Compute.InstanceID), State: et.VolumeAttachmentStateAttached}}
				f.volumes[v.ID] = actual
			}
		}
	}
	c := &Controller{Profiles: map[string]Profile{"profile@1": p}, Store: durable.Store{Root: t.TempDir()}, Clients: func(context.Context, Profile) (EC2API, SSMAPI, STSAPI, error) {
		return f, fakeSSM{}, fakeIdentity{}, nil
	}}
	return c, f, Request{ProfileConfigHash: ProfileHash(p), JobID: "job-1", OwnerID: "owner-1", ProfileKey: "profile@1", NodeID: "node-1", Attempt: 1, PlanHash: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
}
func TestManagedLostResponsesAndCleanup(t *testing.T) {
	c, f, r := fixture(t, "managed")
	ctx := context.Background()
	probe, e := c.Probe(ctx, r)
	if e != nil || !probe.Ready || probe.TargetReady {
		t.Fatal(probe, e)
	}
	if f.launches != 0 || f.creates != 0 {
		t.Fatal("preflight mutated resources")
	}
	f.lostLaunch = true
	if _, e = c.Acquire(ctx, r); e == nil {
		t.Fatal("missing injected failure")
	}
	if _, e = c.Acquire(ctx, r); e != nil {
		t.Fatal(e)
	}
	if f.launches != 1 {
		t.Fatal("duplicate instance")
	}
	f.lostCreate = true
	if _, e = c.PrepareStorage(ctx, r.JobID); e == nil {
		t.Fatal("missing volume failure")
	}
	for i := 0; i < 3; i++ {
		if _, e = c.PrepareStorage(ctx, r.JobID); e != nil {
			t.Fatal(e)
		}
	}
	if f.creates != 1 || f.attaches != 1 {
		t.Fatal("duplicate resources")
	}
	if e = c.RecordOutcome(ctx, r.JobID, "succeeded", "succeeded", true); e != nil {
		t.Fatal(e)
	}
	var state State
	for i := 0; i < 6; i++ {
		state, e = c.Cleanup(ctx, r.JobID)
		if e != nil {
			t.Fatal(e)
		}
		if state.CleanupOutcome == "complete" {
			break
		}
	}
	if state.CleanupOutcome != "complete" || f.terminates != 1 || f.deletes != 1 {
		t.Fatal(state.CleanupOutcome, f.terminates, f.deletes)
	}
}

func TestCleanupAfterOperatorDeletesObservedOwnedVolume(t *testing.T) {
	c, f, r := fixture(t, "managed")
	ctx := context.Background()
	if _, e := c.Acquire(ctx, r); e != nil {
		t.Fatal(e)
	}
	var state State
	for i := 0; i < 3; i++ {
		var e error
		state, e = c.PrepareStorage(ctx, r.JobID)
		if e != nil {
			t.Fatal(e)
		}
	}
	if !state.Volumes[0].Ready {
		t.Fatal("fixture volume was not observed attached")
	}
	delete(f.volumes, state.Volumes[0].ID)
	if e := c.RecordOutcome(ctx, r.JobID, "succeeded", "succeeded", true); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 4; i++ {
		var e error
		state, e = c.Cleanup(ctx, r.JobID)
		if e != nil {
			t.Fatal(e)
		}
	}
	if state.CleanupOutcome != "complete" || state.Volumes[0].CleanupState != "deleted" || f.deletes != 0 {
		t.Fatal(state.CleanupOutcome, state.Volumes, f.deletes)
	}
}
func TestExistingPreservationAndAliasLease(t *testing.T) {
	for _, name := range []string{"existing", "mounted"} {
		t.Run(name, func(t *testing.T) {
			c, f, r := fixture(t, name)
			ctx := context.Background()
			if _, e := c.Acquire(ctx, r); e != nil {
				t.Fatal(e)
			}
			c.Profiles["alias@1"] = c.Profiles[r.ProfileKey]
			alias := r
			alias.ProfileKey = "alias@1"
			alias.JobID = "job-2"
			if _, e := c.Acquire(ctx, alias); e == nil {
				t.Fatal("alias bypassed lease")
			}
			for i := 0; i < 3; i++ {
				if _, e := c.PrepareStorage(ctx, r.JobID); e != nil {
					t.Fatal(e)
				}
			}
			if e := c.RecordOutcome(ctx, r.JobID, "succeeded", "succeeded", false); e != nil {
				t.Fatal(e)
			}
			if _, e := c.Cleanup(ctx, r.JobID); e == nil {
				t.Fatal("unknown process released resources")
			}
			_ = c.RecordOutcome(ctx, r.JobID, "succeeded", "succeeded", true)
			for i := 0; i < 3; i++ {
				if _, e := c.Cleanup(ctx, r.JobID); e != nil {
					t.Fatal(e)
				}
			}
			if f.terminates != 0 || f.deletes != 0 {
				t.Fatal("existing resources destroyed")
			}
			if name == "mounted" && (f.attaches != 0 || f.modifies != 0) {
				t.Fatal("existing mount settings changed")
			}
			if _, e := c.Acquire(ctx, alias); e != nil {
				t.Fatal("finished job did not release lease", e)
			}
		})
	}
}
func TestWrongAZAndOwnershipFailClosed(t *testing.T) {
	c, f, r := fixture(t, "existing")
	v := f.volumes["vol-0123456789abcdef0"]
	v.AvailabilityZone = aws.String("us-west-2b")
	f.volumes["vol-0123456789abcdef0"] = v
	probe, e := c.Probe(context.Background(), r)
	if e != nil || probe.Ready {
		t.Fatal("AZ mismatch accepted", e)
	}
	c, f, r = fixture(t, "managed")
	_, e = c.Acquire(context.Background(), r)
	if e != nil {
		t.Fatal(e)
	}
	_ = c.RecordOutcome(context.Background(), r.JobID, "succeeded", "succeeded", true)
	i := f.instances["i-0123456789abcdef0"]
	i.Tags = nil
	f.instances["i-0123456789abcdef0"] = i
	if _, e = c.Cleanup(context.Background(), r.JobID); e == nil {
		t.Fatal("ownership mismatch ignored")
	}
}

func TestProfilePinAndDurableCapacityAcrossJobs(t *testing.T) {
	c, f, r := fixture(t, "managed")
	p := c.Profiles[r.ProfileKey]
	p.Limits.Concurrency = 1
	c.Profiles[r.ProfileKey] = p
	if _, e := c.Acquire(context.Background(), r); e == nil {
		t.Fatal("changed profile accepted under old pin")
	}
	r.ProfileConfigHash = ProfileHash(p)
	if _, e := c.Acquire(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	second := r
	second.JobID = "job-2"
	state, e := c.Acquire(context.Background(), second)
	if e != nil || state.Phase != "waiting_capacity" || f.launches != 1 {
		t.Fatal(state, e, f.launches)
	}
	restarted := *c
	state, e = restarted.Acquire(context.Background(), second)
	if e != nil || state.Phase != "waiting_capacity" || f.launches != 1 {
		t.Fatal("restart lost capacity evidence", state, e)
	}
}

func TestPhysicalLeaseAcrossConcurrentControllers(t *testing.T) {
	c, _, r := fixture(t, "existing")
	p := c.Profiles[r.ProfileKey]
	gate := make(chan struct{})
	results := make(chan error, 2)
	for _, id := range []string{"job-A", "job-B"} {
		go func(id string) {
			copy := *c
			<-gate
			results <- copy.acquireLease(context.Background(), p, p.Compute.InstanceID, id)
		}(id)
	}
	close(gate)
	wins := 0
	for range 2 {
		if <-results == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("physical resource has %d owners", wins)
	}
}

func TestSuccessfulRetainedVolumeDoesNotRetainCompute(t *testing.T) {
	c, f, r := fixture(t, "managed")
	p := c.Profiles[r.ProfileKey]
	p.Volumes[0].Retention.OnSuccess = "retain"
	c.Profiles[r.ProfileKey] = p
	r.ProfileConfigHash = ProfileHash(p)
	ctx := context.Background()
	if _, e := c.Acquire(ctx, r); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		if _, e := c.PrepareStorage(ctx, r.JobID); e != nil {
			t.Fatal(e)
		}
	}
	if e := c.RecordOutcome(ctx, r.JobID, "succeeded", "succeeded", true); e != nil {
		t.Fatal(e)
	}
	var state State
	for i := 0; i < 5; i++ {
		var e error
		state, e = c.Cleanup(ctx, r.JobID)
		if e != nil {
			t.Fatal(e)
		}
	}
	if state.CleanupOutcome != "retained" || state.RetainUntil == "" || f.terminates != 1 || f.deletes != 0 {
		t.Fatal(state.CleanupOutcome, f.terminates, f.deletes)
	}
	if e := c.Store.With(ctx, stateKey(r.JobID), func(entry *durable.Entry) error {
		var s State
		if e := entry.Read(&s); e != nil {
			return e
		}
		s.RetainUntil = "2000-01-01T00:00:00Z"
		return entry.Write(s)
	}); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		var e error
		state, e = c.Cleanup(ctx, r.JobID)
		if e != nil {
			t.Fatal(e)
		}
	}
	if state.CleanupOutcome != "complete" || f.terminates != 1 || f.deletes != 1 {
		t.Fatal(state.CleanupOutcome, f.terminates, f.deletes)
	}
}

func TestPersistedExistingVolumeIntentDoesNotBypassLease(t *testing.T) {
	c, f, r := fixture(t, "existing")
	ctx := context.Background()
	p := c.Profiles[r.ProfileKey]
	if _, e := c.Acquire(ctx, r); e != nil {
		t.Fatal(e)
	}
	if e := c.acquireLease(ctx, p, p.Volumes[0].ID, "another-job"); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if _, e := c.PrepareStorage(ctx, r.JobID); e == nil {
			t.Fatal("volume intent bypassed another job's physical lease")
		}
	}
	if f.attaches != 0 {
		t.Fatal("leased volume was attached")
	}
}

func TestUnexpectedAttachmentBlocksManagedTermination(t *testing.T) {
	c, f, r := fixture(t, "managed")
	ctx := context.Background()
	state, e := c.Acquire(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	external := "vol-aaaaaaaaaaaaaaaaa"
	i := f.instances[state.InstanceID]
	i.BlockDeviceMappings = append(i.BlockDeviceMappings, et.InstanceBlockDeviceMapping{DeviceName: aws.String("/dev/sdz"), Ebs: &et.EbsInstanceBlockDevice{VolumeId: aws.String(external), DeleteOnTermination: aws.Bool(true)}})
	f.instances[state.InstanceID] = i
	f.volumes[external] = et.Volume{VolumeId: aws.String(external), State: et.VolumeStateInUse, Attachments: []et.VolumeAttachment{{InstanceId: aws.String(state.InstanceID)}}}
	if e = c.RecordOutcome(ctx, r.JobID, "succeeded", "succeeded", true); e != nil {
		t.Fatal(e)
	}
	if _, e = c.Cleanup(ctx, r.JobID); e == nil {
		t.Fatal("unowned volume was put at risk by termination")
	}
	if f.terminates != 0 || f.deletes != 0 {
		t.Fatal("unowned attachment was modified")
	}
}
