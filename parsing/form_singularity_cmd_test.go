package parsing

import (
	"strings"
	"testing"
)

func TestFormSingularityCmdPreservesSingleQuotesInCommand(t *testing.T) {
	rawCommand := "printf '%s\\n' 'DRY RUN' > /tmp/result.txt"
	template := CmdTemplate{BaseCmd: []string{rawCommand}}

	cmd, _ := FormSingularityCmd(template, map[string]string{}, "/tmp/image.sif", false)
	wantSuffix := "sh -c " + shellQuoteArg(rawCommand+"  ")
	if !strings.HasSuffix(cmd, wantSuffix) {
		t.Fatalf("command suffix = %q, want suffix %q", cmd, wantSuffix)
	}
	if !strings.Contains(cmd, "'\\''%s\\n'\\''") {
		t.Fatalf("single quotes were not escaped for the outer shell: %q", cmd)
	}
}
