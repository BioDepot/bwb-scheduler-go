package parsing

import (
	"testing"
)

func TestExecutorsValidation(t *testing.T) {
	valid := []byte(`{
		"executors": {
			"slurm": {"ip_addr":"127.0.0.1","transfer_addr":"127.0.0.1","user":"me","sched_dir":"/root/idk"},
			"local": {},
			"temporal": {}
		},
		"configs": {},
		"node_configs": {}
	}`)
	if _, err := ParseAndValidateJobConfig(valid); err != nil {
		t.Errorf("expected valid executors, got error: %v", err)
	}

	invalidKey := []byte(`{"executors":{"invalid":{}}}`)
	if _, err := ParseAndValidateJobConfig(invalidKey); err == nil {
		t.Errorf("expected error for invalid executor key")
	}

	invalidLocal := []byte(`{"executors":{"slurm":{}}}`)
	if _, err := ParseAndValidateJobConfig(invalidLocal); err == nil {
		t.Errorf("expected error for missing SshConfig fields")
	}

	invalidNonEmptySlurm := []byte(`{"executors":{"local":{"foo":"bar"}}}`)
	if _, err := ParseAndValidateJobConfig(invalidNonEmptySlurm); err == nil {
		t.Errorf("expected error for non-empty local executor")
	}
}

func TestConfigsValidation(t *testing.T) {
	missingExecutor := []byte(`{
		"executors": {"local": {"host":"127.0.0.1","port":22,"user":"me","sched_dir":"/root/123"}},
		"configs": {"cfg1": {"executor":"slurm"}},
		"node_configs": {}
	}`)
	if _, err := ParseAndValidateJobConfig(missingExecutor); err == nil {
		t.Errorf("expected error for slurm config without annotations")
	}

	invalidAnnotations := []byte(`{
		"executors": {"slurm": {"host":"127.0.0.1","port":22,"user":"me","sched_dir":"/root/123"}, "slurm": {}},
		"configs": {"cfg1": {"executor":"slurm","annotations":{"partition":"","time":"","nodes":0}}},
		"node_configs": {}
	}`)
	if _, err := ParseAndValidateJobConfig(invalidAnnotations); err == nil {
		t.Errorf("expected error for incomplete SlurmJobConfig")
	}

	unexpectedAnnotations := []byte(`{
		"executors": {"local": {"host":"127.0.0.1","port":22,"user":"me","sched_dir":"/root/123"}},
		"configs": {"cfg1": {"executor":"local","annotations":{"foo":"bar"}}},
		"node_configs": {}
	}`)
	if _, err := ParseAndValidateJobConfig(unexpectedAnnotations); err == nil {
		t.Errorf("expected error for annotations on non-slurm config")
	}
}

func TestSlurmEnvironmentValidation(t *testing.T) {
	valid := []byte(`{
		"executors": {"slurm": {"ip_addr":"127.0.0.1","transfer_addr":"127.0.0.1","user":"me","sched_dir":"/root/123"}},
		"configs": {"cfg1": {"executor":"slurm","annotations":{"partition":"normal","environment":{"APPTAINERENV_CUDA_VISIBLE_DEVICES":"1"}}}},
		"node_configs": {"1":"cfg1"}
	}`)
	if _, err := ParseAndValidateJobConfig(valid); err != nil {
		t.Fatalf("expected valid environment, got error: %v", err)
	}

	invalidName := []byte(`{
		"executors": {"slurm": {"ip_addr":"127.0.0.1","transfer_addr":"127.0.0.1","user":"me","sched_dir":"/root/123"}},
		"configs": {"cfg1": {"executor":"slurm","annotations":{"partition":"normal","environment":{"BAD-NAME":"1"}}}},
		"node_configs": {"1":"cfg1"}
	}`)
	if _, err := ParseAndValidateJobConfig(invalidName); err == nil {
		t.Fatal("expected invalid environment variable name to fail")
	}
}

func TestExtendedSlurmSiteProfileValidation(t *testing.T) {
	valid := []byte(`{
		"executors": {"slurm": {
			"ip_addr":"cluster.example.org", "command_port":2222,
			"transfer_addr":"transfer.example.org", "transfer_port":22,
			"user":"me", "sched_dir":"/ocean/project/scheduler",
			"known_hosts_file":"/home/me/.ssh/known_hosts",
			"expected_host_key_fingerprint":"SHA256:abc",
			"project_filesystem_roots":["/ocean/project"],
			"allowed_transfer_roots":["/ocean/project"],
			"container_runtime":"apptainer"
		}},
		"configs": {"cfg1": {"executor":"slurm","annotations":{
			"partition":"RM-shared", "account":"project-1", "qos":"normal",
			"reservation":"reserved-1", "time":"00:30:00", "nodes":1,
			"ntasks":2, "tasks_per_node":2, "cpus_per_task":8, "mem":"32G", "gpus":"1"
		}}},
		"node_configs": {"1":"cfg1"}
	}`)
	config, err := ParseAndValidateJobConfig(valid)
	if err != nil {
		t.Fatalf("extended site profile rejected: %v", err)
	}
	if config.SlurmExecutor.CommandEndpoint() != "cluster.example.org:2222" ||
		config.SlurmExecutor.ContainerRuntime != "apptainer" {
		t.Fatalf("extended executor fields were not retained: %#v", config.SlurmExecutor)
	}

	unknown := []byte(`{
		"executors": {"slurm": {"ip_addr":"host","transfer_addr":"host","user":"me","sched_dir":"/root","proxy_jump":"unsupported"}},
		"configs": {}, "node_configs": {}
	}`)
	if _, err := ParseAndValidateJobConfig(unknown); err == nil {
		t.Fatal("unsupported site-profile field was silently accepted")
	}
}

func TestNodeConfigsValidation(t *testing.T) {
	valid := []byte(`{
		"executors": {"slurm": {"ip_addr":"127.0.0.1","transfer_addr":"127.0.0.1","user":"me","sched_dir":"/root/123"}},
		"configs": {"cfg1": {"executor":"slurm","annotations":{"partition": "123"}}},
		"node_configs": {"1":"cfg1"}
	}`)
	if _, err := ParseAndValidateJobConfig(valid); err != nil {
		t.Errorf("expected valid node_configs, got error: %v", err)
	}

	missingPartition := []byte(`{
		"executors": {"slurm": {"ip_addr":"127.0.0.1","transfer_addr":"127.0.0.1","user":"me","sched_dir":"/root/123"}},
		"configs": {"cfg1": {"executor":"slurm","annotations":{}}},
		"node_configs": {"1":"cfg1"}
	}`)
	if _, err := ParseAndValidateJobConfig(missingPartition); err == nil {
		t.Errorf("expected error for missing SLURM partition")
	}

	nonIntegerKey := []byte(`{
		"executors": {"local": {"ip_addr":"127.0.0.1","transfer_addr":"127.0.0.1","user":"me","sched_dir":"/root/123"}},
		"configs": {"cfg1": {"executor":"local","value":{}}},
		"node_configs": {"abc":"cfg1"}
	}`)
	if _, err := ParseAndValidateJobConfig(nonIntegerKey); err == nil {
		t.Errorf("expected error for non-integer node_configs key")
	}

	unknownConfig := []byte(`{
		"executors": {"local": {"ip_addr":"127.0.0.1","transfer_addr":"127.0.0.1","user":"me","sched_dir":"/root/123"}},
		"configs": {"cfg1": {"executor":"local","value":{}}},
		"node_configs": {"1":"cfg2"}
	}`)
	if _, err := ParseAndValidateJobConfig(unknownConfig); err == nil {
		t.Errorf("expected error for node_configs referencing unknown config")
	}
}

func TestInvalidJSON(t *testing.T) {
	badJSON := []byte(`{ executors: }`)
	if _, err := ParseAndValidateJobConfig(badJSON); err == nil {
		t.Errorf("expected error for invalid JSON input")
	}
}

func TestValidateTime(t *testing.T) {
	minutes := "123"
	if err := validateWalltimeStr(minutes); err != nil {
		t.Fatalf("incorrectly rejected [minutes] time string")
	}

	minutesSeconds := "123:22"
	if err := validateWalltimeStr(minutesSeconds); err != nil {
		t.Fatalf("incorrectly rejected [minutes:seconds] time string")
	}
	minutesSecondsOverflow := "123:221"
	if err := validateWalltimeStr(minutesSecondsOverflow); err == nil {
		t.Fatalf("incorrectly accepted overflowing [minutes:seconds] time string")
	}

	hoursMinutesSeconds := "12:23:22"
	if err := validateWalltimeStr(hoursMinutesSeconds); err != nil {
		t.Fatalf("incorrectly rejected [hours:minutes:seconds] time string")
	}

	hoursMinutesSecondsOverflow := "12:23:221"
	if err := validateWalltimeStr(hoursMinutesSecondsOverflow); err == nil {
		t.Fatalf("incorrectly accepted overflowing [hours:minutes:seconds] time string")
	}

	daysHours := "12-12"
	if err := validateWalltimeStr(daysHours); err != nil {
		t.Fatalf("incorrectly rejected [days-hours] time string")
	}

	daysHoursMinutes := "12-12:12"
	if err := validateWalltimeStr(daysHoursMinutes); err != nil {
		t.Fatalf("incorrectly rejected [days-hours:minutes] time string")
	}
	daysHoursMinutesOverflow := "12-12:121"
	if err := validateWalltimeStr(daysHoursMinutesOverflow); err == nil {
		t.Fatalf("incorrectly accepted overflowing [days-hours:minutes] time string")
	}

	daysHoursMinutesSeconds := "12-12:12:12"
	if err := validateWalltimeStr(daysHoursMinutesSeconds); err != nil {
		t.Fatalf("incorrectly rejected [days-hours:minutes:seconds] time string")
	}

	daysHoursMinutesSecondsOverflow := "12-12:12:121"
	if err := validateWalltimeStr(daysHoursMinutesSecondsOverflow); err == nil {
		t.Fatalf("incorrectly accepted overflowing [days-hours:minutes:seconds] time string")
	}

	invalidStr := "12h00"
	if err := validateWalltimeStr(invalidStr); err == nil {
		t.Fatalf("incorrectly accepted invalid time str %s", invalidStr)
	}
}

func TestOverallParsing(t *testing.T) {
	valid := []byte(`{
		"executors": {"slurm": {"ip_addr":"127.0.0.1","transfer_addr":"127.0.0.1","user":"me","sched_dir":"/root/123"}},
		"configs": {"cfg1": {"executor":"slurm","annotations":{"partition": "123"}}},
		"node_configs": {"1":"cfg1"}
	}`)
	_, err := ParseAndValidateJobConfig(valid)
	if err != nil {
		t.Errorf("expected valid node_configs, got error: %v", err)
	}
}
