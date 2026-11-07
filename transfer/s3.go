package transfer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	st "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"go-scheduler/durable"
)

type S3API interface {
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	ListMultipartUploads(context.Context, *s3.ListMultipartUploadsInput, ...func(*s3.Options)) (*s3.ListMultipartUploadsOutput, error)
}
type S3Store struct {
	Root       string
	Client     S3API
	Journal    durable.Store
	PartSize   int64
	Encryption st.ServerSideEncryption
	KMSKey     string
	Progress   func(context.Context, int64)
}
type uploadState struct {
	Source      Artifact
	Destination string
	UploadID    string
	Creating    bool
	Parts       []st.CompletedPart
	Complete    bool
}

func notFound(err error) bool {
	var e smithy.APIError
	return errors.As(err, &e) && (e.ErrorCode() == "NotFound" || e.ErrorCode() == "NoSuchKey" || e.ErrorCode() == "404")
}

// Without ListBucket, S3 also reports a missing key as 403. This is only
// permission to try a conditional create, never evidence that the key is absent.
func ambiguousHead(err error) bool {
	var e smithy.APIError
	return errors.As(err, &e) && (e.ErrorCode() == "Forbidden" || e.ErrorCode() == "AccessDenied" || e.ErrorCode() == "403")
}
func (s *S3Store) verifyRemote(ctx context.Context, a Artifact) (Artifact, error) {
	b, k, err := SplitS3(a.URI)
	if err != nil {
		return Artifact{}, err
	}
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(k), VersionId: optional(a.VersionID)})
	if err != nil {
		return Artifact{}, err
	}
	defer out.Body.Close()
	h, n, err := Hash(s.reader(ctx, out.Body))
	if err != nil {
		return Artifact{}, err
	}
	if h != a.SHA256 || n != a.Size {
		return Artifact{}, fmt.Errorf("remote object checksum or size conflict")
	}
	a.VersionID = aws.ToString(out.VersionId)
	return a, nil
}
func optional(v string) *string {
	if v == "" {
		return nil
	}
	return aws.String(v)
}
func (s *S3Store) Upload(ctx context.Context, id string, source Artifact, destination string) (Artifact, error) {
	b, k, err := SplitS3(destination)
	if err != nil {
		return Artifact{}, err
	}
	f, err := s.open(source.URI)
	if err != nil {
		return Artifact{}, err
	}
	defer f.Close()
	hash, size, err := Hash(s.reader(ctx, f))
	if err != nil {
		return Artifact{}, err
	}
	if _, err = f.Seek(0, 0); err != nil {
		return Artifact{}, err
	}
	if hash != source.SHA256 || size != source.Size {
		return Artifact{}, fmt.Errorf("source bytes changed")
	}
	result := Artifact{URI: destination, SHA256: source.SHA256, Size: source.Size}
	err = s.Journal.With(ctx, "s3-upload/"+id, func(e *durable.Entry) error {
		var state uploadState
		if err := e.Read(&state); err == nil {
			if state.Source != source || state.Destination != destination {
				return fmt.Errorf("upload identity conflict")
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		state.Source, state.Destination = source, destination
		// Recover an accepted Complete/Put response before issuing another mutation.
		_, headErr := s.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(k)})
		if headErr == nil {
			var err error
			result, err = s.verifyRemote(ctx, result)
			if err != nil {
				return err
			}
			state.Complete = true
			return e.Write(state)
		}
		if !notFound(headErr) && !ambiguousHead(headErr) {
			return headErr
		}
		// Both PutObject and CompleteMultipartUpload below require IfNoneMatch=*.
		// A denied existence probe can therefore never authorize an overwrite;
		// publication still requires a successful byte-for-byte readback.
		partSize := s.PartSize
		if partSize < 5*1024*1024 {
			partSize = 8 * 1024 * 1024
		}
		if source.Size/partSize >= 10000 {
			return fmt.Errorf("artifact exceeds configured multipart part count")
		}
		if source.Size < partSize {
			if err = e.Write(state); err != nil {
				return err
			}
			_, err = s.Client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(b), Key: aws.String(k), Body: f, IfNoneMatch: aws.String("*"), ContentLength: aws.Int64(source.Size), ChecksumSHA256: aws.String(base64Digest(source.SHA256)), ServerSideEncryption: s.Encryption, SSEKMSKeyId: optional(s.KMSKey), Metadata: map[string]string{"sha256": source.SHA256}})
			if err != nil {
				return err
			}
		} else {
			if state.UploadID == "" && state.Creating {
				var keyMarker, uploadMarker *string
				for {
					out, err := s.Client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(b), Prefix: aws.String(k), KeyMarker: keyMarker, UploadIdMarker: uploadMarker})
					if err != nil {
						return err
					}
					for _, u := range out.Uploads {
						if aws.ToString(u.Key) == k {
							if state.UploadID != "" {
								return fmt.Errorf("ambiguous incomplete uploads require reconciliation")
							}
							state.UploadID = aws.ToString(u.UploadId)
						}
					}
					if !aws.ToBool(out.IsTruncated) {
						break
					}
					keyMarker, uploadMarker = out.NextKeyMarker, out.NextUploadIdMarker
				}
			}
			if state.UploadID == "" {
				state.Creating = true
				if err = e.Write(state); err != nil {
					return err
				}
				out, err := s.Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(k), ChecksumAlgorithm: st.ChecksumAlgorithmSha256, ChecksumType: st.ChecksumTypeComposite, ServerSideEncryption: s.Encryption, SSEKMSKeyId: optional(s.KMSKey), Metadata: map[string]string{"sha256": source.SHA256}})
				if err != nil {
					return err
				}
				state.UploadID = aws.ToString(out.UploadId)
			}
			if err = e.Write(state); err != nil {
				return err
			}
			full := sha256.New()
			parts := []st.CompletedPart{}
			cachedParts := append([]st.CompletedPart{}, state.Parts...)
			for offset, number := int64(0), int32(1); offset < source.Size; offset, number = offset+partSize, number+1 {
				if err = ctx.Err(); err != nil {
					return err
				}
				size := partSize
				if source.Size-offset < size {
					size = source.Size - offset
				}
				section := io.NewSectionReader(f, offset, size)
				partHash := sha256.New()
				if _, err = io.Copy(io.MultiWriter(full, partHash), section); err != nil {
					return err
				}
				checksum := base64.StdEncoding.EncodeToString(partHash.Sum(nil))
				if int(number) <= len(cachedParts) && aws.ToString(cachedParts[number-1].ChecksumSHA256) == checksum {
					parts = append(parts, cachedParts[number-1])
				} else {
					out, err := s.Client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(b), Key: aws.String(k), UploadId: aws.String(state.UploadID), PartNumber: aws.Int32(number), Body: io.NewSectionReader(f, offset, size), ContentLength: aws.Int64(size), ChecksumSHA256: aws.String(checksum)})
					if err != nil {
						var apiErr smithy.APIError
						if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchUpload" {
							state.UploadID = ""
							state.Parts = nil
							state.Creating = false
							if writeErr := e.Write(state); writeErr != nil {
								return writeErr
							}
						}
						return err
					}
					if out.ChecksumSHA256 != nil && aws.ToString(out.ChecksumSHA256) != checksum {
						return fmt.Errorf("uploaded part checksum mismatch")
					}
					parts = append(parts, st.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(number), ChecksumSHA256: aws.String(checksum)})
				}
				state.Parts = append([]st.CompletedPart{}, parts...)
				if err = e.Write(state); err != nil {
					return err
				}
				if s.Progress != nil {
					s.Progress(ctx, offset+size)
				}
			}
			if fmt.Sprintf("sha256:%x", full.Sum(nil)) != source.SHA256 {
				return fmt.Errorf("source changed during multipart upload")
			}
			_, err = s.Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(k), UploadId: aws.String(state.UploadID), IfNoneMatch: aws.String("*"), ChecksumType: st.ChecksumTypeComposite, MultipartUpload: &st.CompletedMultipartUpload{Parts: parts}})
			if err != nil {
				return err
			}
		}
		result, err = s.verifyRemote(ctx, result)
		if err != nil {
			return err
		}
		state.Complete = true
		return e.Write(state)
	})
	return result, err
}
func base64Digest(h string) string {
	var bytes [32]byte
	for i := range bytes {
		fmt.Sscanf(h[7+i*2:9+i*2], "%02x", &bytes[i])
	}
	return base64.StdEncoding.EncodeToString(bytes[:])
}

// Abort only the exact journaled upload. Completed objects are never deleted.
func (s *S3Store) Abort(ctx context.Context, id string) error {
	return s.Journal.With(ctx, "s3-upload/"+id, func(e *durable.Entry) error {
		var state uploadState
		if err := e.Read(&state); err != nil {
			return err
		}
		if state.Complete || state.UploadID == "" {
			return nil
		}
		b, k, err := SplitS3(state.Destination)
		if err != nil {
			return err
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_, err = s.Client.AbortMultipartUpload(cleanup, &s3.AbortMultipartUploadInput{Bucket: aws.String(b), Key: aws.String(k), UploadId: aws.String(state.UploadID)})
		if err != nil {
			return err
		}
		state.UploadID = ""
		state.Parts = nil
		state.Creating = false
		return e.Write(state)
	})
}
func (s *S3Store) Download(ctx context.Context, source Artifact, destination string) (Artifact, error) {
	if existing, err := FileArtifactWithin(s.root(destination), destination); err == nil {
		if existing.SHA256 != source.SHA256 || existing.Size != source.Size {
			return Artifact{}, fmt.Errorf("destination bytes conflict")
		}
		return existing, nil
	} else if !os.IsNotExist(err) {
		return Artifact{}, err
	}
	b, k, err := SplitS3(source.URI)
	if err != nil {
		return Artifact{}, err
	}
	parent, err := s.parent(destination)
	if err != nil {
		return Artifact{}, err
	}
	defer parent.Close()
	handle, err := parent.Open(".")
	if err != nil {
		return Artifact{}, err
	}
	defer handle.Close()
	out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(k), VersionId: optional(source.VersionID)})
	if err != nil {
		return Artifact{}, err
	}
	defer out.Body.Close()
	name := fmt.Sprintf(".download-%x", randomBytes())
	f, err := parent.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return Artifact{}, err
	}
	defer parent.Remove(name)
	defer f.Close()
	h, n, err := Hash(s.reader(ctx, io.TeeReader(out.Body, f)))
	if err != nil {
		return Artifact{}, err
	}
	if h != source.SHA256 || n != source.Size {
		return Artifact{}, fmt.Errorf("download checksum or size mismatch")
	}
	if err = f.Sync(); err != nil {
		return Artifact{}, err
	}
	if err = f.Close(); err != nil {
		return Artifact{}, err
	}
	// Link publishes atomically without replacing another writer's destination.
	if err = unix.Linkat(int(handle.Fd()), name, int(handle.Fd()), filepath.Base(destination), 0); err != nil {
		return Artifact{}, err
	}
	if err = handle.Sync(); err != nil {
		return Artifact{}, err
	}
	return Artifact{URI: destination, SHA256: h, Size: n, VersionID: aws.ToString(out.VersionId)}, nil
}

func randomBytes() []byte {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return b
}
func (s *S3Store) root(p string) string {
	if s.Root != "" {
		return s.Root
	}
	return filepath.Dir(p)
}
func (s *S3Store) open(p string) (*os.File, error) {
	root := s.root(p)
	if !Within(p, root) {
		return nil, fmt.Errorf("path outside root")
	}
	dir, e := os.OpenRoot(root)
	if e != nil {
		return nil, e
	}
	defer dir.Close()
	rel, e := filepath.Rel(root, p)
	if e != nil {
		return nil, e
	}
	f, e := dir.Open(rel)
	if e != nil {
		return nil, e
	}
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, fmt.Errorf("source is not a regular file")
	}
	return f, nil
}
func (s *S3Store) parent(p string) (*os.Root, error) {
	root := s.root(p)
	if !Within(p, root) {
		return nil, fmt.Errorf("path outside root")
	}
	dir, e := os.OpenRoot(root)
	if e != nil {
		return nil, e
	}
	defer dir.Close()
	rel, e := filepath.Rel(root, filepath.Dir(p))
	if e != nil {
		return nil, e
	}
	current := ""
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		if e = dir.Mkdir(current, 0700); e != nil && !os.IsExist(e) {
			return nil, e
		}
	}
	return dir.OpenRoot(rel)
}

type progressReader struct {
	ctx      context.Context
	r        io.Reader
	progress func(context.Context, int64)
	n        int64
	last     time.Time
}

func (r *progressReader) Read(b []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	n, e := r.r.Read(b)
	r.n += int64(n)
	if r.progress != nil && time.Since(r.last) > time.Second {
		r.progress(r.ctx, r.n)
		r.last = time.Now()
	}
	return n, e
}
func (s *S3Store) reader(ctx context.Context, r io.Reader) io.Reader {
	return &progressReader{ctx: ctx, r: r, progress: s.Progress}
}
