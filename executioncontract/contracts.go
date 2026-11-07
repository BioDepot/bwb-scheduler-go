// Package executioncontract validates offline Workbench placement/storage plans.
// A valid document is not permission to execute. Worker policy must be loaded
// from operator configuration, never supplied as authorization by the caller.
package executioncontract

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	AWSProfileSchema = "morphic.aws_ec2_profile/v1"
	PlacementSchema  = "morphic.execution_placement/v1"
	StorageSchema    = "morphic.storage_plan/v1"
	PlanSchema       = "morphic.execution_plan/v1"
	PolicySchema     = "morphic.execution_worker_policy/v1"
	MaxDocumentBytes = 2 * 1024 * 1024
)

//go:embed schema.json
var schemaJSON []byte
var compileOnce sync.Once
var compiled *jsonschema.Schema
var compileError error
var intPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)

type Document = map[string]any

func object(v any) Document { return v.(map[string]any) }
func list(v any) []any      { return v.([]any) }
func str(v any) string      { return v.(string) }

func schema() (*jsonschema.Schema, error) {
	compileOnce.Do(func() {
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaJSON))
		if err != nil {
			compileError = err
			return
		}
		c := jsonschema.NewCompiler()
		if err = c.AddResource("https://biodepot.org/schemas/execution-contracts-v1.json", doc); err != nil {
			compileError = err
			return
		}
		compiled, compileError = c.Compile("https://biodepot.org/schemas/execution-contracts-v1.json")
	})
	return compiled, compileError
}

// Decode rejects duplicate keys, fractional numbers, invalid Unicode and
// excessive nesting before either hashing or schema validation.
func Decode(raw []byte) (any, error) {
	if len(raw) > MaxDocumentBytes || !utf8.Valid(raw) {
		return nil, fmt.Errorf("invalid contract size or UTF-8")
	}
	if err := unicodeEscapes(raw); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	v, err := decodeValue(dec, 0)
	if err != nil {
		return nil, err
	}
	if _, err = dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON content")
	}
	return v, nil
}

func unicodeEscapes(raw []byte) error {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if i >= len(raw) || raw[i] != 'u' {
			continue
		}
		if i+4 >= len(raw) {
			return fmt.Errorf("incomplete Unicode escape")
		}
		n, err := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		if err != nil {
			return err
		}
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return fmt.Errorf("unpaired Unicode surrogate")
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return fmt.Errorf("unpaired Unicode surrogate")
			}
			low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return fmt.Errorf("unpaired Unicode surrogate")
			}
			i += 6
		}
	}
	return nil
}

func decodeValue(d *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, fmt.Errorf("contract nesting exceeds 64 levels")
	}
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			m := Document{}
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return nil, err
				}
				key, ok := k.(string)
				if !ok {
					return nil, fmt.Errorf("invalid object key")
				}
				if _, ok = m[key]; ok {
					return nil, fmt.Errorf("duplicate JSON object key")
				}
				v, err := decodeValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				m[key] = v
			}
			if _, err = d.Token(); err != nil {
				return nil, err
			}
			return m, nil
		case '[':
			a := []any{}
			for d.More() {
				v, err := decodeValue(d, depth+1)
				if err != nil {
					return nil, err
				}
				a = append(a, v)
			}
			if _, err = d.Token(); err != nil {
				return nil, err
			}
			return a, nil
		default:
			return nil, fmt.Errorf("unexpected delimiter")
		}
	}
	if n, ok := t.(json.Number); ok {
		if _, err := integer(n); err != nil {
			return nil, err
		}
	}
	return t, nil
}

func integer(n json.Number) (int64, error) {
	if !intPattern.MatchString(string(n)) {
		return 0, fmt.Errorf("integer numbers required")
	}
	i, err := strconv.ParseInt(string(n), 10, 64)
	if err != nil || i > 9007199254740991 || i < -9007199254740991 {
		return 0, fmt.Errorf("integer exceeds interoperable range")
	}
	return i, nil
}

// CanonicalHash matches Python sorted-key ensure_ascii JSON. It deliberately
// supports integer numbers only, avoiding cross-language float formatting.
func CanonicalHash(v any) (string, error) {
	var b bytes.Buffer
	if err := canonical(&b, v, 0); err != nil {
		return "", err
	}
	if b.Len() > MaxDocumentBytes {
		return "", fmt.Errorf("contract exceeds 2 MiB")
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(b.Bytes())), nil
}
func quote(b *bytes.Buffer, s string) error {
	if !utf8.ValidString(s) {
		return fmt.Errorf("invalid UTF-8")
	}
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 32 || r >= 127 {
				if r > 0xffff {
					hi, lo := utf16.EncodeRune(r)
					fmt.Fprintf(b, `\u%04x\u%04x`, hi, lo)
				} else {
					fmt.Fprintf(b, `\u%04x`, r)
				}
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return nil
}
func canonical(b *bytes.Buffer, v any, depth int) error {
	if depth > 64 {
		return fmt.Errorf("contract nesting exceeds 64 levels")
	}
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		if x {
			b.WriteString("true")
		} else {
			b.WriteString("false")
		}
	case string:
		return quote(b, x)
	case json.Number:
		i, err := integer(x)
		if err != nil {
			return err
		}
		fmt.Fprint(b, i)
	case []any:
		b.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := canonical(b, item, depth+1); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := quote(b, k); err != nil {
				return err
			}
			b.WriteByte(':')
			if err := canonical(b, x[k], depth+1); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	default:
		return fmt.Errorf("unsupported JSON type %T; decode with executioncontract.Decode", v)
	}
	return nil
}

func hash(v any) string { h, _ := CanonicalHash(v); return h }
func unique(items any, key, label string) (map[string]Document, error) {
	result := map[string]Document{}
	for _, item := range list(items) {
		m := object(item)
		k := str(m[key])
		if _, ok := result[k]; ok {
			return nil, fmt.Errorf("duplicate %s", label)
		}
		result[k] = m
	}
	return result, nil
}
func pathCheck(p string, prefix bool) error {
	p = strings.TrimPrefix(p, "/")
	if prefix {
		if !strings.HasSuffix(p, "/") {
			return fmt.Errorf("storage prefixes require trailing slash")
		}
		p = strings.TrimSuffix(p, "/")
	}
	for _, part := range strings.Split(p, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("paths must be normalized without traversal")
		}
	}
	return nil
}
func s3Check(uri string, prefix bool) error {
	parts := strings.SplitN(uri[5:], "/", 2)
	bucket := parts[0]
	if strings.Contains(bucket, "..") || strings.HasPrefix(bucket, "xn--") || strings.HasSuffix(bucket, "-s3alias") || strings.HasSuffix(bucket, "--ol-s3") {
		return fmt.Errorf("invalid S3 bucket")
	}
	return pathCheck(parts[1], prefix)
}
func within(p, root string) bool {
	r := strings.TrimRight(root, "/")
	return p == r || strings.HasPrefix(p, r+"/")
}
func inRoots(p string, roots any) bool {
	for _, r := range list(roots) {
		if within(p, str(r)) {
			return true
		}
	}
	return false
}

func profileCheck(p Document) error {
	s := object(p["s3"])
	_, hasKMS := s["kms_key_arn"]
	if (s["encryption"] == "aws:kms") != hasKMS {
		return fmt.Errorf("KMS encryption requires exactly one key")
	}
	for _, prefix := range append(append([]any{}, list(s["input_prefixes"])...), s["output_prefix"]) {
		if err := s3Check(str(prefix), true); err != nil {
			return err
		}
	}
	account := str(p["account_id"])
	if role, present := p["controller_role_arn"]; present && !strings.Contains(str(role), ":iam::"+account+":role/") {
		return fmt.Errorf("controller role account mismatch")
	}
	c := object(p["compute"])
	if c["mode"] == "managed" && !strings.Contains(str(c["instance_profile_arn"]), ":iam::"+account+":instance-profile/") {
		return fmt.Errorf("instance profile account mismatch")
	}
	mounts := []string{}
	volumes := map[string]bool{}
	writable := false
	kmsItems := []Document{s}
	for _, v := range list(p["volumes"]) {
		vol := object(v)
		if vol["mode"] == "mounted" && c["mode"] != "existing" {
			return fmt.Errorf("mounted volumes require an existing instance")
		}
		mount := str(vol["mount_path"])
		if err := pathCheck(mount, false); err != nil {
			return err
		}
		for _, prev := range mounts {
			if within(mount, prev) || within(prev, mount) {
				return fmt.Errorf("overlapping EBS mounts")
			}
		}
		mounts = append(mounts, mount)
		if !vol["read_only"].(bool) {
			writable = true
		}
		if vol["mode"] != "new" {
			if object(vol["retention"])["on_success"] != "retain" {
				return fmt.Errorf("existing EBS must be retained")
			}
			id := str(vol["volume_id"])
			if volumes[id] {
				return fmt.Errorf("duplicate EBS volume")
			}
			volumes[id] = true
		}
		kmsItems = append(kmsItems, vol)
	}
	if !writable {
		return fmt.Errorf("EBS work volume must be writable")
	}
	if (c["mode"] == "existing" || len(volumes) > 0) && strNumber(object(p["limits"])["max_concurrent_jobs"]) != "1" {
		return fmt.Errorf("existing instances and volumes require exclusive use")
	}
	for _, item := range kmsItems {
		if key, ok := item["kms_key_arn"]; ok && !strings.Contains(str(key), ":kms:"+str(p["region"])+":"+account+":key/") {
			return fmt.Errorf("KMS key account or region mismatch")
		}
	}
	return nil
}
func strNumber(v any) string { return string(v.(json.Number)) }

func placementCheck(p Document) (map[string]Document, error) {
	nodes, err := unique(p["nodes"], "node_id", "node ID")
	if err != nil {
		return nil, err
	}
	keys, refs := map[string]string{}, map[string]string{}
	for id, n := range nodes {
		key := str(n["executor_key"])
		if key == "s3" {
			return nil, fmt.Errorf("s3 is reserved")
		}
		binding := str(n["kind"]) + hash(n["profile"])
		if prev, ok := keys[key]; ok && prev != binding {
			return nil, fmt.Errorf("conflicting executor key")
		}
		keys[key] = binding
		pid := str(object(n["profile"])["profile_id"])
		if prev, ok := refs[pid]; ok && prev != binding {
			return nil, fmt.Errorf("conflicting profile pin")
		}
		refs[pid] = binding
		for _, dep := range list(n["depends_on"]) {
			if _, ok := nodes[str(dep)]; !ok || str(dep) == id {
				return nil, fmt.Errorf("unknown or self dependency")
			}
		}
	}
	remaining := map[string]bool{}
	for id := range nodes {
		remaining[id] = true
	}
	for len(remaining) > 0 {
		ready := []string{}
		for id := range remaining {
			blocked := false
			for _, dep := range list(nodes[id]["depends_on"]) {
				if remaining[str(dep)] {
					blocked = true
				}
			}
			if !blocked {
				ready = append(ready, id)
			}
		}
		if len(ready) == 0 {
			return nil, fmt.Errorf("placement contains cycle")
		}
		for _, id := range ready {
			delete(remaining, id)
		}
	}
	return nodes, nil
}

func storageCheck(s Document, nodes map[string]Document) error {
	arts, err := unique(s["artifacts"], "artifact_id", "artifact ID")
	if err != nil {
		return err
	}
	routes, err := unique(s["routes"], "route_id", "route ID")
	if err != nil {
		return err
	}
	for _, a := range arts {
		src := object(a["source"])
		if src["kind"] == "s3" {
			err = s3Check(str(src["uri"]), false)
		} else {
			err = pathCheck(str(src["path"]), false)
		}
		if err != nil {
			return err
		}
		if nodes != nil {
			if src["kind"] == "stage_output" {
				if _, ok := nodes[str(src["node_id"])]; !ok {
					return fmt.Errorf("unknown producer")
				}
			}
			if key, ok := src["executor_key"]; ok {
				found := false
				for _, n := range nodes {
					if n["executor_key"] == key {
						found = true
					}
				}
				if !found {
					return fmt.Errorf("unknown source executor")
				}
			}
		}
	}
	for _, r := range routes {
		if err = s3Check(str(r["staging_prefix"]), true); err != nil {
			return err
		}
	}
	targets := map[string][]string{}
	for _, v := range list(s["transfers"]) {
		t := object(v)
		target := str(t["target_path"])
		if err = pathCheck(target, false); err != nil {
			return err
		}
		a, aok := arts[str(t["artifact_id"])]
		r, rok := routes[str(t["route_id"])]
		if !aok || !rok {
			return fmt.Errorf("unknown transfer artifact or route")
		}
		to := str(t["to_node"])
		for _, prev := range targets[to] {
			if within(target, prev) || within(prev, target) {
				return fmt.Errorf("overlapping transfer targets")
			}
		}
		targets[to] = append(targets[to], target)
		if nodes == nil {
			continue
		}
		dest, ok := nodes[to]
		if !ok {
			return fmt.Errorf("unknown consumer")
		}
		src := object(a["source"])
		sourceKey := "s3"
		if key, ok := src["executor_key"]; ok {
			sourceKey = str(key)
		}
		if src["kind"] == "stage_output" {
			anc := map[string]bool{}
			pending := append([]any{}, list(dest["depends_on"])...)
			for len(pending) > 0 {
				id := str(pending[0])
				pending = pending[1:]
				if !anc[id] {
					anc[id] = true
					pending = append(pending, list(nodes[id]["depends_on"])...)
				}
			}
			producer := str(src["node_id"])
			if !anc[producer] {
				return fmt.Errorf("producer must be dependency")
			}
			sourceKey = str(nodes[producer]["executor_key"])
		}
		if r["source_executor_key"] != sourceKey || r["destination_executor_key"] != dest["executor_key"] {
			return fmt.Errorf("route endpoint mismatch")
		}
	}
	for _, v := range list(s["publications"]) {
		p := object(v)
		if err = pathCheck(str(p["path"]), false); err != nil {
			return err
		}
		if err = s3Check(str(p["destination_prefix"]), true); err != nil {
			return err
		}
		if nodes != nil {
			if _, ok := nodes[str(p["node_id"])]; !ok {
				return fmt.Errorf("unknown publication node")
			}
		}
	}
	return nil
}

func ExecutionFingerprint(plan Document) (string, error) {
	m := Document{}
	for k, v := range plan {
		if k != "review" {
			m[k] = v
		}
	}
	return CanonicalHash(m)
}

// Validate decodes and validates one of the versioned shared contracts.
func Validate(raw []byte) (Document, error) {
	v, err := Decode(raw)
	if err != nil {
		return nil, err
	}
	if _, err = CanonicalHash(v); err != nil {
		return nil, err
	}
	validator, err := schema()
	if err != nil {
		return nil, err
	}
	if err = validator.Validate(v); err != nil {
		return nil, fmt.Errorf("execution contract schema violation")
	}
	p := object(v)
	switch p["schema"] {
	case AWSProfileSchema:
		err = profileCheck(p)
	case PlacementSchema:
		_, err = placementCheck(p)
	case StorageSchema:
		err = storageCheck(p, nil)
	case PlanSchema:
		var nodes map[string]Document
		nodes, err = placementCheck(object(p["placement"]))
		if err == nil {
			err = storageCheck(object(p["storage"]), nodes)
		}
		if err == nil {
			err = checkpointsCheck(p, nodes)
		}
		if err == nil {
			fp, _ := ExecutionFingerprint(p)
			if object(p["review"])["state_hash"] != fp {
				err = fmt.Errorf("review does not bind current plan")
			}
		}
	case PolicySchema:
		pins := map[string]bool{}
		for _, v := range list(p["profiles"]) {
			site := object(v)
			key := hash(site["reference"])
			if pins[key] {
				return nil, fmt.Errorf("duplicate policy profile")
			}
			pins[key] = true
			_, hasAWS := site["aws"]
			if (site["kind"] == "aws_ec2") != hasAWS {
				return nil, fmt.Errorf("AWS policy entry requires configuration")
			}
			for _, r := range list(site["allowed_roots"]) {
				if err = pathCheck(str(r), false); err != nil {
					return nil, err
				}
			}
			if hasAWS {
				if err = profileCheck(object(site["aws"])); err != nil {
					return nil, err
				}
			}
		}
		if _, err = unique(p["routes"], "route_id", "policy route"); err != nil {
			return nil, err
		}
		for _, v := range list(p["routes"]) {
			if err = s3Check(str(object(v)["staging_prefix"]), true); err != nil {
				return nil, err
			}
		}
		for _, v := range append(append([]any{}, list(p["input_prefixes"])...), list(p["publication_prefixes"])...) {
			if err = s3Check(str(v), true); err != nil {
				return nil, err
			}
		}
	}
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Authorize checks a plan against an operator-supplied immutable site allowlist.
func Authorize(planRaw, policyRaw []byte) (Document, error) {
	p, err := Validate(planRaw)
	if err != nil {
		return nil, err
	}
	policy, err := Validate(policyRaw)
	if err != nil {
		return nil, err
	}
	if p["schema"] != PlanSchema || policy["schema"] != PolicySchema {
		return nil, fmt.Errorf("expected execution plan and worker policy")
	}
	sites, byNode, byKey := map[string]Document{}, map[string]Document{}, map[string]Document{}
	for _, v := range list(policy["profiles"]) {
		s := object(v)
		sites[hash(s["reference"])] = s
	}
	for _, v := range list(object(p["placement"])["nodes"]) {
		n := object(v)
		s, ok := sites[hash(n["profile"])]
		if !ok || s["kind"] != n["kind"] {
			return nil, fmt.Errorf("profile revision not allowed")
		}
		byNode[str(n["node_id"])] = s
		byKey[str(n["executor_key"])] = s
	}
	routes, _ := unique(policy["routes"], "route_id", "route")
	storage := object(p["storage"])
	for _, v := range list(storage["routes"]) {
		r := object(v)
		allowed, ok := routes[str(r["route_id"])]
		if !ok || hash(allowed) != hash(r) {
			return nil, fmt.Errorf("transfer route not allowed")
		}
	}
	arts := map[string]Document{}
	for _, v := range list(storage["artifacts"]) {
		a := object(v)
		s := object(a["source"])
		arts[str(a["artifact_id"])] = s
		if s["kind"] == "s3" {
			if !inRoots(str(s["uri"]), policy["input_prefixes"]) {
				return nil, fmt.Errorf("S3 input prefix not allowed")
			}
		} else if s["kind"] == "server_file" || s["kind"] == "ebs_path" {
			site := byKey[str(s["executor_key"])]
			roots := site["allowed_roots"]
			if s["kind"] == "ebs_path" {
				if site["kind"] != "aws_ec2" {
					return nil, fmt.Errorf("EBS requires AWS executor")
				}
				mounts := []any{}
				for _, v := range list(object(site["aws"])["volumes"]) {
					mounts = append(mounts, object(v)["mount_path"])
				}
				roots = mounts
			}
			if !inRoots(str(s["path"]), roots) {
				return nil, fmt.Errorf("input outside approved roots")
			}
		}
	}
	for _, v := range list(storage["transfers"]) {
		t := object(v)
		src := arts[str(t["artifact_id"])]
		r := routes[str(t["route_id"])]
		uri := str(r["staging_prefix"])
		if src["kind"] == "s3" {
			uri = str(src["uri"])
		}
		dest := byNode[str(t["to_node"])]
		if dest["kind"] == "aws_ec2" && !inRoots(uri, object(object(dest["aws"])["s3"])["input_prefixes"]) {
			return nil, fmt.Errorf("AWS input prefix not allowed")
		}
		if src["kind"] != "s3" {
			var s Document
			if src["kind"] == "stage_output" {
				s = byNode[str(src["node_id"])]
			} else {
				s = byKey[str(src["executor_key"])]
			}
			if s["kind"] == "aws_ec2" && !within(str(r["staging_prefix"]), str(object(object(s["aws"])["s3"])["output_prefix"])) {
				return nil, fmt.Errorf("AWS staging prefix not allowed")
			}
		}
	}
	for _, v := range list(storage["publications"]) {
		o := object(v)
		prefix := str(o["destination_prefix"])
		s := byNode[str(o["node_id"])]
		if !inRoots(prefix, policy["publication_prefixes"]) {
			return nil, fmt.Errorf("publication prefix not allowed")
		}
		if s["kind"] == "aws_ec2" && !within(prefix, str(object(object(s["aws"])["s3"])["output_prefix"])) {
			return nil, fmt.Errorf("AWS publication prefix not allowed")
		}
	}
	return p, nil
}

type Submission struct {
	RequestID   string `json:"request_id"`
	WorkflowID  string `json:"workflow_id"`
	Fingerprint string `json:"fingerprint"`
}

func SubmissionRecord(raw []byte) (Submission, error) {
	p, err := Validate(raw)
	if err != nil {
		return Submission{}, err
	}
	if p["schema"] != PlanSchema {
		return Submission{}, fmt.Errorf("expected plan")
	}
	id := object(p["identity"])
	return Submission{str(id["request_id"]), str(id["workflow_id"]), hash(p)}, nil
}
func CheckRetry(prior Submission, raw []byte) error {
	candidate, err := SubmissionRecord(raw)
	if err != nil {
		return err
	}
	if prior != candidate {
		return fmt.Errorf("submission identity or content conflicts")
	}
	return nil
}

// ExecutionIDs identifies a logical command attempt, independent of transport
// retries. The caller must persist attempt changes as a newly reviewed plan.
func ExecutionIDs(raw []byte, nodeID, commandID string) (Document, error) {
	p, err := Validate(raw)
	if err != nil {
		return nil, err
	}
	if p["schema"] != PlanSchema {
		return nil, fmt.Errorf("expected plan")
	}
	found := false
	for _, n := range list(object(p["placement"])["nodes"]) {
		if object(n)["node_id"] == nodeID {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("unknown node")
	}
	if !regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`).MatchString(commandID) {
		return nil, fmt.Errorf("invalid command ID")
	}
	identity := object(p["identity"])
	node := Document{}
	for k, v := range identity {
		node[k] = v
	}
	node["node_id"] = nodeID
	nodeHash := hash(node)
	node["command_id"] = commandID
	return Document{"parent_workflow_id": identity["workflow_id"], "node_id": nodeID,
		"command_id": commandID, "attempt": identity["attempt"],
		"node_workflow_id": "node-" + nodeHash[7:], "command_key": "command-" + hash(node)[7:]}, nil
}
