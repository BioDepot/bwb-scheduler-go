package transfer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	st "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"go-scheduler/durable"
)

type fakeS3 struct {
	S3API
	objects         map[string][]byte
	parts           map[int32][]byte
	upload          string
	key             string
	created         int
	put             int
	uploaded        []int32
	failPart        int32
	loseCreate      bool
	loseComplete    bool
	aborted         bool
	missingHeadCode string
	denyAllHeads    bool
}

func fake() *fakeS3 { return &fakeS3{objects: map[string][]byte{}, parts: map[int32][]byte{}} }
func (f *fakeS3) HeadObject(_ context.Context, in *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if f.denyAllHeads {
		return nil, &smithy.GenericAPIError{Code: "Forbidden"}
	}
	if _, ok := f.objects[*in.Key]; !ok {
		if f.missingHeadCode != "" {
			return nil, &smithy.GenericAPIError{Code: f.missingHeadCode}
		}
		return nil, &smithy.GenericAPIError{Code: "NotFound"}
	}
	return &s3.HeadObjectOutput{}, nil
}

func TestUploadWithoutBucketListPermissionKeepsConditionalWrites(t *testing.T) {
	for _, size := range []int{20, 6 * 1024 * 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			s, f, a := setup(t, size)
			f.missingHeadCode = "Forbidden"
			uri := "s3://example-bucket/attempt-1/a"
			got, e := s.Upload(context.Background(), "first", a, uri)
			if e != nil || got.SHA256 != a.SHA256 {
				t.Fatal(got, e)
			}
			f.denyAllHeads = true
			f.objects["attempt-1/a"] = []byte("previous content")
			if _, e = s.Upload(context.Background(), "conflict", a, uri); e == nil {
				t.Fatal("denied HEAD authorized overwrite")
			}
			if string(f.objects["attempt-1/a"]) != "previous content" {
				t.Fatal("existing object changed")
			}
		})
	}
}
func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	b, ok := f.objects[*in.Key]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey"}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(b)), VersionId: aws.String("version-1")}, nil
}
func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if _, ok := f.objects[*in.Key]; ok {
		return nil, fmt.Errorf("precondition")
	}
	if aws.ToString(in.IfNoneMatch) != "*" {
		panic("missing write condition")
	}
	b, e := io.ReadAll(in.Body)
	f.objects[*in.Key] = b
	f.put++
	return &s3.PutObjectOutput{}, e
}
func (f *fakeS3) CreateMultipartUpload(_ context.Context, in *s3.CreateMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	f.created++
	f.upload = "upload-1"
	f.key = *in.Key
	if f.loseCreate {
		f.loseCreate = false
		return nil, fmt.Errorf("lost create response")
	}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(f.upload)}, nil
}
func (f *fakeS3) ListMultipartUploads(_ context.Context, _ *s3.ListMultipartUploadsInput, _ ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error) {
	return &s3.ListMultipartUploadsOutput{Uploads: []st.MultipartUpload{{Key: aws.String(f.key), UploadId: aws.String(f.upload)}}}, nil
}
func (f *fakeS3) UploadPart(_ context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if f.failPart == *in.PartNumber {
		f.failPart = 0
		return nil, fmt.Errorf("interrupted upload")
	}
	b, e := io.ReadAll(in.Body)
	f.parts[*in.PartNumber] = b
	f.uploaded = append(f.uploaded, *in.PartNumber)
	return &s3.UploadPartOutput{ETag: aws.String("etag"), ChecksumSHA256: in.ChecksumSHA256}, e
}
func (f *fakeS3) CompleteMultipartUpload(_ context.Context, in *s3.CompleteMultipartUploadInput, _ ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if aws.ToString(in.IfNoneMatch) != "*" {
		panic("missing write condition")
	}
	if _, ok := f.objects[*in.Key]; ok {
		return nil, fmt.Errorf("precondition")
	}
	var all []byte
	for _, p := range in.MultipartUpload.Parts {
		all = append(all, f.parts[*p.PartNumber]...)
	}
	f.objects[*in.Key] = all
	if f.loseComplete {
		f.loseComplete = false
		return nil, fmt.Errorf("lost complete response")
	}
	return &s3.CompleteMultipartUploadOutput{}, nil
}
func (f *fakeS3) AbortMultipartUpload(_ context.Context, _ *s3.AbortMultipartUploadInput, _ ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	f.aborted = true
	return &s3.AbortMultipartUploadOutput{}, nil
}
func setup(t *testing.T, size int) (*S3Store, *fakeS3, Artifact) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "source")
	if e := os.WriteFile(p, bytes.Repeat([]byte("a"), size), 0600); e != nil {
		t.Fatal(e)
	}
	a, e := FileArtifact(p)
	if e != nil {
		t.Fatal(e)
	}
	f := fake()
	return &S3Store{Client: f, Journal: durable.Store{Root: t.TempDir()}, PartSize: 5 * 1024 * 1024, Encryption: st.ServerSideEncryptionAes256}, f, a
}
func TestMultipartResumeAndLostResponses(t *testing.T) {
	for _, failure := range []string{"part", "create", "complete"} {
		t.Run(failure, func(t *testing.T) {
			s, f, a := setup(t, 6*1024*1024)
			f.failPart = 0
			f.loseCreate = failure == "create"
			f.loseComplete = failure == "complete"
			if failure == "part" {
				f.failPart = 2
			}
			uri := "s3://example-bucket/attempt-1/artifact"
			if _, e := s.Upload(context.Background(), "x", a, uri); e == nil {
				t.Fatal("expected injected failure")
			}
			// Simulate a worker process restart with only the durable journal retained.
			next := *s
			got, e := next.Upload(context.Background(), "x", a, uri)
			if e != nil {
				t.Fatal(e)
			}
			if got.SHA256 != a.SHA256 || f.created != 1 {
				t.Fatal("duplicate or corrupt upload")
			}
			destination := filepath.Join(t.TempDir(), "received")
			if _, e = s.Download(context.Background(), got, destination); e != nil {
				t.Fatal(e)
			}
			b, e := FileArtifact(destination)
			if e != nil || b.SHA256 != a.SHA256 {
				t.Fatal("round trip mismatch")
			}
			if len(f.uploaded) != 2 {
				t.Fatal("uploaded completed parts again", f.uploaded)
			}
		})
	}
}
func TestConflictsAndAbort(t *testing.T) {
	s, f, a := setup(t, 6*1024*1024)
	f.failPart = 2
	_, _ = s.Upload(context.Background(), "x", a, "s3://example-bucket/attempt-1/a")
	if e := s.Abort(context.Background(), "x"); e != nil || !f.aborted {
		t.Fatal("incomplete upload not aborted", e)
	}
	s, f, a = setup(t, 20)
	uri := "s3://example-bucket/attempt-1/a"
	if _, e := s.Upload(context.Background(), "x", a, uri); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Upload(context.Background(), "x", a, uri); e != nil || f.put != 1 {
		t.Fatal("duplicate put", e)
	}
	f.objects["attempt-1/a"] = []byte("corrupt")
	if _, e := s.Upload(context.Background(), "x", a, uri); e == nil {
		t.Fatal("corruption accepted")
	}
}
func TestEndpointIsolationAndAttemptScope(t *testing.T) {
	s, _, a := setup(t, 20)
	root := filepath.Dir(a.URI)
	w := Worker{Objects: s, Journal: durable.Store{Root: t.TempDir()}, Endpoints: map[string]Endpoint{"worker-a": {Key: "worker-a", Root: root}, "s3": {Key: "s3", WritePrefixes: []string{"s3://example-bucket/staging/"}}}}
	r := Request{ID: "transfer-1", AttemptID: "attempt-1", SourceEndpoint: "worker-a", DestinationEndpoint: "s3", Source: a, Destination: "s3://example-bucket/staging/attempt-1/data"}
	if _, e := w.Transfer(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	r.SourceEndpoint = "worker-b"
	if e := w.Validate(r); e == nil {
		t.Fatal("unknown endpoint accepted")
	}
	r.SourceEndpoint = "worker-a"
	r.Destination = "s3://example-bucket/staging/other/data"
	if e := w.Validate(r); e == nil {
		t.Fatal("unscoped destination accepted")
	}
}

func TestRootedArtifactReadRejectsEscapingSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("outside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	if _, err := FileArtifactWithin(root, filepath.Join(root, "escape", "secret")); err == nil {
		t.Fatal("read escaped root")
	}
}
