package transfer

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	st "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go-scheduler/durable"
)

// Command is delivered by a trusted stage worker to the pinned executor binary,
// never accepted as authorization from a public HTTP request.
type Command struct {
	Request        Request  `json:"request"`
	Root           string   `json:"root"`
	JournalRoot    string   `json:"journal_root"`
	Region         string   `json:"region"`
	InputPrefixes  []string `json:"input_prefixes"`
	OutputPrefixes []string `json:"output_prefixes"`
	Encryption     string   `json:"encryption"`
	KMSKey         string   `json:"kms_key,omitempty"`
	Publish        bool     `json:"publish"`
}

func ExecuteCommand(ctx context.Context, c Command) (Receipt, error) {
	cfg, e := config.LoadDefaultConfig(ctx, config.WithRegion(c.Region), config.WithHTTPClient(&http.Client{Timeout: 24 * time.Hour}), config.WithRetryer(func() aws.Retryer { return retry.AddWithMaxAttempts(retry.NewStandard(), 3) }))
	if e != nil {
		return Receipt{}, e
	}
	if c.Publish {
		if strings.HasPrefix(c.Request.Source.URI, "s3://") {
			return Receipt{}, fmt.Errorf("publication requires a local source")
		}
		a, e := FileArtifactWithin(c.Root, c.Request.Source.URI)
		if e != nil {
			return Receipt{}, e
		}
		c.Request.Source = a
	}
	journal := durable.Store{Root: c.JournalRoot}
	objects := &S3Store{Client: s3.NewFromConfig(cfg), Journal: journal, Encryption: st.ServerSideEncryption(c.Encryption), KMSKey: c.KMSKey}
	local := Endpoint{Root: c.Root}
	remote := Endpoint{ReadPrefixes: c.InputPrefixes, WritePrefixes: c.OutputPrefixes}
	endpoints := map[string]Endpoint{}
	if strings.HasPrefix(c.Request.Source.URI, "s3://") {
		endpoints[c.Request.SourceEndpoint] = remote
		endpoints[c.Request.DestinationEndpoint] = local
	} else {
		endpoints[c.Request.SourceEndpoint] = local
		endpoints[c.Request.DestinationEndpoint] = remote
	}
	worker := Worker{Endpoints: endpoints, Objects: objects, Journal: journal}
	return worker.Transfer(ctx, c.Request)
}
