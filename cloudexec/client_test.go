package cloudexec

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// Exercise the real SDK provider chain and request signing against local HTTP
// fixtures. These tests neither read the operator's keys nor contact AWS.
func TestSDKNamedProfileAuthentication(t *testing.T) {
	for _, test := range []struct {
		name, mode string
		role, deny bool
	}{
		{name: "direct", mode: "default_chain"},
		{name: "legacy_role", role: true},
		{name: "explicit_role", mode: "assume_role", role: true},
		{name: "denied_role_never_falls_back", role: true, deny: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const sourceKey = "AKIAFIXTURESOURCEONLY"
			const assumedKey = "ASIAFIXTUREASSUMEDKEY"
			const roleARN = "arn:aws:iam::123456789012:role/reviewed-controller"
			var mu sync.Mutex
			calls := map[string]int{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := r.ParseForm(); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				action := r.FormValue("Action")
				if strings.HasSuffix(r.Header.Get("X-Amz-Target"), ".DescribeInstanceInformation") {
					action = "DescribeInstanceInformation"
				}
				mu.Lock()
				calls[action]++
				mu.Unlock()
				key := sourceKey
				if test.role && action != "AssumeRole" {
					key = assumedKey
				}
				auth := r.Header.Get("Authorization")
				if !strings.Contains(auth, "Credential="+key+"/") || !strings.Contains(auth, "/us-east-2/") {
					t.Error("request used an unexpected identity or region")
				}
				w.Header().Set("Content-Type", "text/xml")
				switch action {
				case "AssumeRole":
					if r.FormValue("RoleArn") != roleARN {
						t.Error("request substituted the reviewed role")
					}
					if test.deny {
						w.WriteHeader(http.StatusForbidden)
						fmt.Fprint(w, `<ErrorResponse><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>Fixture denial</Message></Error><RequestId>fixture</RequestId></ErrorResponse>`)
						return
					}
					fmt.Fprintf(w, `<AssumeRoleResponse><AssumeRoleResult><Credentials><AccessKeyId>%s</AccessKeyId><SecretAccessKey>fixture-assumed-secret</SecretAccessKey><SessionToken>fixture-session</SessionToken><Expiration>%s</Expiration></Credentials><AssumedRoleUser><AssumedRoleId>fixture</AssumedRoleId><Arn>arn:aws:sts::123456789012:assumed-role/reviewed-controller/fixture</Arn></AssumedRoleUser></AssumeRoleResult></AssumeRoleResponse>`, assumedKey, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
				case "GetCallerIdentity":
					fmt.Fprint(w, `<GetCallerIdentityResponse><GetCallerIdentityResult><Account>123456789012</Account><Arn>arn:aws:iam::123456789012:user/fixture</Arn><UserId>fixture</UserId></GetCallerIdentityResult></GetCallerIdentityResponse>`)
				case "DescribeInstances":
					fmt.Fprint(w, `<DescribeInstancesResponse><requestId>fixture</requestId><reservationSet/></DescribeInstancesResponse>`)
				case "DescribeInstanceInformation":
					w.Header().Set("Content-Type", "application/x-amz-json-1.1")
					fmt.Fprint(w, `{"InstanceInformationList":[]}`)
				default:
					t.Errorf("unexpected API action %q", action)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			root := t.TempDir()
			credentials := filepath.Join(root, "credentials")
			configuration := filepath.Join(root, "config")
			if err := os.WriteFile(credentials, []byte("[default]\naws_access_key_id=decoy\naws_secret_access_key=decoy\n[fixture-selected]\naws_access_key_id="+sourceKey+"\naws_secret_access_key=fixture-source-secret\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(configuration, []byte("[profile fixture-selected]\nregion=us-west-1\n"), 0600); err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CA_BUNDLE"} {
				t.Setenv(name, "")
			}
			for name, value := range map[string]string{
				"AWS_PROFILE": "fixture-selected", "AWS_DEFAULT_PROFILE": "fixture-selected",
				"AWS_SHARED_CREDENTIALS_FILE": credentials, "AWS_CONFIG_FILE": configuration,
				"AWS_EC2_METADATA_DISABLED": "true", "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "false",
				"AWS_ENDPOINT_URL_STS": server.URL, "AWS_ENDPOINT_URL_EC2": server.URL, "AWS_ENDPOINT_URL_SSM": server.URL,
			} {
				t.Setenv(name, value)
			}
			p := Profile{AuthMode: test.mode, Region: "us-east-2"}
			if test.role {
				p.Role = roleARN
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ec2Client, ssmClient, identity, err := SDKClients(ctx, p)
			if err != nil {
				t.Fatal(err)
			}
			_, err = identity.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
			if test.deny {
				if err == nil {
					t.Fatal("denied role assumption unexpectedly succeeded")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if _, err = ec2Client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{}); err != nil {
					t.Fatal(err)
				}
				if _, err = ssmClient.DescribeInstanceInformation(ctx, &ssm.DescribeInstanceInformationInput{}); err != nil {
					t.Fatal(err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if test.deny {
				if calls["AssumeRole"] != 1 || len(calls) != 1 {
					t.Fatalf("denied role made unexpected requests: %v", calls)
				}
			} else {
				wantAssume := 0
				if test.role {
					wantAssume = 1
				}
				if calls["AssumeRole"] != wantAssume || calls["GetCallerIdentity"] != 1 || calls["DescribeInstances"] != 1 || calls["DescribeInstanceInformation"] != 1 {
					t.Fatalf("unexpected credential exchange/API requests: %v", calls)
				}
			}
		})
	}
}

func TestSDKRejectsAmbiguousAuthentication(t *testing.T) {
	for _, p := range []Profile{
		{},
		{AuthMode: "assume_role"},
		{AuthMode: "default_chain", Role: "arn:aws:iam::123456789012:role/controller"},
		{AuthMode: "unexpected"},
	} {
		if _, _, _, err := SDKClients(context.Background(), p); err == nil {
			t.Fatal("ambiguous authentication accepted")
		}
	}
}
