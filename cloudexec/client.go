package cloudexec

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

func SDKClients(ctx context.Context, p Profile) (EC2API, SSMAPI, STSAPI, error) {
	// An omitted mode preserves the original role-based contract. Direct use of
	// the operator-selected SDK credential chain must be explicitly reviewed.
	switch p.AuthMode {
	case "", "assume_role":
		if p.Role == "" {
			return nil, nil, nil, fmt.Errorf("controller role required for assume_role authentication")
		}
	case "default_chain":
		if p.Role != "" {
			return nil, nil, nil, fmt.Errorf("default_chain authentication cannot specify a controller role")
		}
	default:
		return nil, nil, nil, fmt.Errorf("unsupported controller authentication mode")
	}
	cfg, e := config.LoadDefaultConfig(ctx, config.WithRegion(p.Region), config.WithHTTPClient(&http.Client{Timeout: 45 * time.Second}), config.WithRetryer(func() aws.Retryer { return retry.AddWithMaxAttempts(retry.NewStandard(), 3) }))
	if e != nil {
		return nil, nil, nil, e
	}
	// Profiles and their authentication mode are immutable operator policy.
	// Requests never carry credentials or choose a local AWS profile. A failed
	// role assumption must never fall back to the source identity's permissions.
	if p.AuthMode != "default_chain" {
		source := sts.NewFromConfig(cfg)
		cfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(source, p.Role))
	}
	return ec2.NewFromConfig(cfg), ssm.NewFromConfig(cfg), sts.NewFromConfig(cfg), nil
}
