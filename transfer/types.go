// Package transfer moves artifacts on explicitly configured workers. The HTTP
// process never proxies artifact bytes. Local endpoints include EBS and Slurm
// gateway mounts; workers must advertise each exact endpoint identity.
package transfer

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"go-scheduler/durable"
)

type Artifact struct {
	URI       string `json:"uri"`
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size_bytes"`
	VersionID string `json:"version_id,omitempty"`
}
type Endpoint struct {
	Key           string   `json:"key"`
	Root          string   `json:"root"`
	ReadPrefixes  []string `json:"read_prefixes"`
	WritePrefixes []string `json:"write_prefixes"`
}
type Request struct {
	ID                  string   `json:"id"`
	AttemptID           string   `json:"attempt_id"`
	SourceEndpoint      string   `json:"source_endpoint"`
	DestinationEndpoint string   `json:"destination_endpoint"`
	Source              Artifact `json:"source"`
	Destination         string   `json:"destination"`
}
type Receipt struct {
	RequestID   string   `json:"request_id"`
	Source      Artifact `json:"source"`
	Destination Artifact `json:"destination"`
	Verified    bool     `json:"verified"`
}
type Worker struct {
	Endpoints map[string]Endpoint
	Objects   *S3Store
	Journal   durable.Store
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var digest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func SplitS3(uri string) (string, string, error) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" {
		return "", "", fmt.Errorf("invalid S3 URI")
	}
	key := strings.TrimPrefix(u.Path, "/")
	if key == "" || path.Clean(key) != key || strings.HasPrefix(key, "../") || strings.Contains(key, "\\") {
		return "", "", fmt.Errorf("noncanonical S3 key")
	}
	return u.Host, key, nil
}
func Within(p, root string) bool {
	return p == strings.TrimSuffix(root, "/") || strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/")
}
func allowed(uri string, prefixes []string) bool {
	for _, p := range prefixes {
		if Within(uri, p) {
			return true
		}
	}
	return false
}
func localPath(ep Endpoint, p string) (string, error) {
	if !filepath.IsAbs(ep.Root) || filepath.Clean(ep.Root) != ep.Root || !filepath.IsAbs(p) || filepath.Clean(p) != p || !Within(p, ep.Root) {
		return "", fmt.Errorf("path outside configured endpoint root")
	}
	// Reject escaping symlinks for existing files and the nearest existing parent.
	parent := p
	for {
		resolved, err := filepath.EvalSymlinks(parent)
		if err == nil {
			root, e := filepath.EvalSymlinks(ep.Root)
			if e != nil || !Within(resolved, root) {
				return "", fmt.Errorf("path escapes endpoint through symlink")
			}
			break
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		if parent == ep.Root {
			return "", err
		}
		parent = filepath.Dir(parent)
	}
	return p, nil
}
func Hash(r io.Reader) (string, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	return fmt.Sprintf("sha256:%x", h.Sum(nil)), n, err
}
func FileArtifact(p string) (Artifact, error) {
	f, e := os.Open(p)
	if e != nil {
		return Artifact{}, e
	}
	defer f.Close()
	h, n, e := Hash(f)
	return Artifact{URI: p, SHA256: h, Size: n}, e
}

// FileArtifactWithin opens through an OS-enforced root, including symlink resolution.
func FileArtifactWithin(root, p string) (Artifact, error) {
	if !filepath.IsAbs(root) || !filepath.IsAbs(p) || !Within(p, root) {
		return Artifact{}, fmt.Errorf("path outside root")
	}
	dir, e := os.OpenRoot(root)
	if e != nil {
		return Artifact{}, e
	}
	defer dir.Close()
	rel, e := filepath.Rel(root, p)
	if e != nil {
		return Artifact{}, e
	}
	f, e := dir.Open(rel)
	if e != nil {
		return Artifact{}, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return Artifact{}, e
	}
	if !info.Mode().IsRegular() {
		return Artifact{}, fmt.Errorf("artifact must be a regular file")
	}
	h, n, e := Hash(f)
	return Artifact{URI: p, SHA256: h, Size: n}, e
}
func (w *Worker) Validate(r Request) error {
	if !identifier.MatchString(r.ID) || !identifier.MatchString(r.AttemptID) || !digest.MatchString(r.Source.SHA256) || r.Source.Size < 0 {
		return fmt.Errorf("invalid transfer identity or digest")
	}
	src, ok := w.Endpoints[r.SourceEndpoint]
	if !ok {
		return fmt.Errorf("source endpoint is not served by this worker")
	}
	dst, ok := w.Endpoints[r.DestinationEndpoint]
	if !ok {
		return fmt.Errorf("destination endpoint is not served by this worker")
	}
	fromS3, toS3 := strings.HasPrefix(r.Source.URI, "s3://"), strings.HasPrefix(r.Destination, "s3://")
	if fromS3 == toS3 {
		return fmt.Errorf("route must connect one local endpoint and S3")
	}
	if fromS3 {
		if _, _, e := SplitS3(r.Source.URI); e != nil {
			return e
		}
		if !allowed(r.Source.URI, src.ReadPrefixes) {
			return fmt.Errorf("S3 source prefix not allowed")
		}
		if _, e := localPath(dst, r.Destination); e != nil {
			return e
		}
		if !strings.Contains(r.Destination, "/"+r.AttemptID+"/") {
			return fmt.Errorf("destination must be scoped to the logical attempt")
		}
	} else {
		if _, e := localPath(src, r.Source.URI); e != nil {
			return e
		}
		if _, _, e := SplitS3(r.Destination); e != nil {
			return e
		}
		if !allowed(r.Destination, dst.WritePrefixes) || !strings.Contains(r.Destination, "/"+r.AttemptID+"/") {
			return fmt.Errorf("S3 destination must be allowlisted and scoped to the logical attempt")
		}
	}
	if w.Objects == nil {
		return fmt.Errorf("S3 client not configured")
	}
	return nil
}
func (w *Worker) Transfer(ctx context.Context, r Request) (Receipt, error) {
	if err := w.Validate(r); err != nil {
		return Receipt{}, err
	}
	var result Receipt
	err := w.Journal.With(ctx, "transfer/"+r.ID, func(e *durable.Entry) error {
		var record struct {
			Request Request  `json:"request"`
			Receipt *Receipt `json:"receipt,omitempty"`
		}
		err := e.Read(&record)
		if err == nil {
			if record.Request != r {
				return fmt.Errorf("transfer identity conflicts with stored request")
			}
			if record.Receipt != nil {
				result = *record.Receipt
				return nil
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		record.Request = r
		if err = e.Write(record); err != nil {
			return err
		}
		var artifact Artifact
		objects := *w.Objects
		if strings.HasPrefix(r.Source.URI, "s3://") {
			objects.Root = w.Endpoints[r.DestinationEndpoint].Root
			artifact, err = objects.Download(ctx, r.Source, r.Destination)
		} else {
			objects.Root = w.Endpoints[r.SourceEndpoint].Root
			artifact, err = objects.Upload(ctx, r.ID, r.Source, r.Destination)
		}
		if err != nil {
			return err
		}
		result = Receipt{r.ID, r.Source, artifact, true}
		record.Receipt = &result
		return e.Write(record)
	})
	return result, err
}
