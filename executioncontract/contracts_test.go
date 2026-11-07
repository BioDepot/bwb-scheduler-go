package executioncontract

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func corpus(t *testing.T) Document {
	t.Helper()
	raw, err := os.ReadFile("testdata/contracts-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	v, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	return object(v)
}
func encoded(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func clone(t *testing.T, v any) Document {
	t.Helper()
	c, err := Decode(encoded(t, v))
	if err != nil {
		t.Fatal(err)
	}
	return object(c)
}
func change(doc Document, changes []any) {
	for _, item := range changes {
		c := object(item)
		// Corpus paths contain no escaped JSON pointer characters.
		keys := strings.Split(str(c["path"])[1:], "/")
		var target any = doc
		for _, key := range keys[:len(keys)-1] {
			if a, ok := target.([]any); ok {
				i, _ := strconv.Atoi(key)
				target = a[i]
			} else {
				target = object(target)[key]
			}
		}
		last := keys[len(keys)-1]
		if a, ok := target.([]any); ok {
			i, _ := strconv.Atoi(last)
			a[i] = c["value"]
		} else {
			object(target)[last] = c["value"]
		}
	}
}

func TestSharedConformance(t *testing.T) {
	c := corpus(t)
	valid := object(c["valid"])
	for name, value := range valid {
		t.Run(name, func(t *testing.T) {
			if _, err := Validate(encoded(t, value)); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, value := range list(c["invalid"]) {
		test := object(value)
		t.Run(str(test["name"]), func(t *testing.T) {
			doc := clone(t, valid[str(test["base"])])
			change(doc, list(test["changes"]))
			if test["refresh_review"] == true {
				h, err := ExecutionFingerprint(doc)
				if err != nil {
					t.Fatal(err)
				}
				object(doc["review"])["state_hash"] = h
			}
			var err error
			if test["authorize"] == true {
				policy := clone(t, valid["policy"])
				if changes, ok := test["policy_changes"]; ok {
					change(policy, list(changes))
				}
				_, err = Authorize(encoded(t, doc), encoded(t, policy))
			} else {
				_, err = Validate(encoded(t, doc))
			}
			if err == nil {
				t.Fatal("invalid contract accepted")
			}
		})
	}
	for _, raw := range list(c["invalid_json"]) {
		if _, err := Decode([]byte(str(raw))); err == nil {
			t.Fatalf("invalid JSON accepted: %s", raw)
		}
	}
}
func TestHashesIdentityAndRetry(t *testing.T) {
	c := corpus(t)
	for _, v := range list(c["canonical"]) {
		vector := object(v)
		h, err := CanonicalHash(vector["value"])
		if err != nil || h != vector["sha256"] {
			t.Fatalf("canonical mismatch: %s %v", h, err)
		}
	}
	valid := object(c["valid"])
	raw := encoded(t, valid["hybrid"])
	if _, err := Authorize(raw, encoded(t, valid["policy"])); err != nil {
		t.Fatal(err)
	}
	ids, err := ExecutionIDs(raw, "align", "cmd-1")
	if err != nil || !reflect.DeepEqual(ids, c["execution_ids"]) {
		t.Fatalf("identity mismatch: %v %v", ids, err)
	}
	record, err := SubmissionRecord(raw)
	if err != nil || record.Fingerprint != c["submission_fingerprint"] {
		t.Fatalf("fingerprint mismatch: %v %v", record, err)
	}
	if err := CheckRetry(record, raw); err != nil {
		t.Fatal(err)
	}
	changed := clone(t, valid["hybrid"])
	object(changed["identity"])["attempt"] = json.Number("2")
	h, _ := ExecutionFingerprint(changed)
	object(changed["review"])["state_hash"] = h
	if err := CheckRetry(record, encoded(t, changed)); err == nil {
		t.Fatal("conflict accepted")
	}
	next, err := ExecutionIDs(encoded(t, changed), "align", "cmd-1")
	if err != nil || next["command_key"] == ids["command_key"] {
		t.Fatal("attempt did not change identity")
	}
}
