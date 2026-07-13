package parsing

import (
    "encoding/json"
    "errors"
    "fmt"
    "os"
    "regexp"
    "strings"
)

type ExecType int

const (
    EXEC_SLURM    = iota
    EXEC_LOCAL    = iota
    EXEC_TEMPORAL = iota
)

type SshConfig struct {
    IpAddr       string  `json:"ip_addr"`
    User         string  `json:"user"`
    TransferAddr string  `json:"transfer_addr"`
    SchedDir     string  `json:"sched_dir"`
    CmdPrefix    *string `json:"cmd_prefix"`
}

type SlurmJobConfig struct {
    MaxRetries  *int              `json:"max_retries,omitempty"`
    Mem         *string           `json:"mem,omitempty"`
    CpusPerTask *int              `json:"cpus_per_task,omitempty"`
    Gpus        *string           `json:"gpus,omitempty"`
    Nodes       *int              `json:"nodes,omitempty"`
    Ntasks      *int              `json:"ntasks,omitempty"`
    Time        *string           `json:"time,omitempty"`
    Partition   *string           `json:"partition,omitempty"`
    Modules     *[]string         `json:"modules,omitempty"`
    Environment map[string]string `json:"environment,omitempty"`
}

type LocalJobConfig struct {
    UseDocker bool
}

type ConfigValue struct {
    Executor    string         `json:"executor"`
    Annotations map[string]any `json:"annotations,omitempty"`
}

type RawJobConfig struct {
    Executors    map[string]any         `json:"executors"`
    Configs      map[string]ConfigValue `json:"configs"`
    NodeConfigs  map[int]string         `json:"node_configs"`
    originalJSON []byte
}

type JobConfig struct {
    ExecTypeByNode     map[int]ExecType
    SlurmExecutor      SshConfig
    SlurmConfigsByNode map[int]SlurmJobConfig
    SlurmConfigsByName map[string]SlurmJobConfig
    // Currently, these executors take no user arguments, but
    // we mark them anyway in case we ever want to add any.
    LocalExecutor         struct{}
    LocalConfigsByNode    map[int]LocalJobConfig
    LocalConfigsByName    map[string]LocalJobConfig
    TemporalExecutor      struct{}
    TemporalConfigsByNode map[int]LocalJobConfig
    TemporalConfigsByName map[string]LocalJobConfig
}

func ParseJobConfig(data []byte, jc *JobConfig) error {
    var jd RawJobConfig
    jd.originalJSON = data

    var temp struct {
        Executors   map[string]interface{} `json:"executors"`
        Configs     map[string]interface{} `json:"configs"`
        NodeConfigs map[int]string         `json:"node_configs"`
    }

    if err := json.Unmarshal(data, &temp); err != nil {
        return fmt.Errorf("failed to parse JSON structure: %w", err)
    }

    jd.Executors = temp.Executors
    jd.NodeConfigs = temp.NodeConfigs

    // Parse configs with proper type handling
    jd.Configs = make(map[string]ConfigValue)
    for key, rawValue := range temp.Configs {
        configBytes, err := json.Marshal(rawValue)
        if err != nil {
            return fmt.Errorf("failed to marshal config '%s': %w", key, err)
        }

        var config ConfigValue
        if err := json.Unmarshal(configBytes, &config); err != nil {
            return fmt.Errorf("failed to parse config '%s': %w", key, err)
        }
        jd.Configs[key] = config
    }

    jc.ExecTypeByNode = make(map[int]ExecType)
    jc.SlurmConfigsByName = make(map[string]SlurmJobConfig)
    jc.SlurmConfigsByNode = make(map[int]SlurmJobConfig)
    jc.LocalConfigsByName = make(map[string]LocalJobConfig)
    jc.LocalConfigsByNode = make(map[int]LocalJobConfig)
    jc.TemporalConfigsByName = make(map[string]LocalJobConfig)
    jc.TemporalConfigsByNode = make(map[int]LocalJobConfig)
    return jd.Validate(jc)
}

func (jd *RawJobConfig) MarshalJSON() ([]byte, error) {
    if len(jd.originalJSON) > 0 {
        return jd.originalJSON, nil
    }

    type MarshalDocument struct {
        Executors   map[string]interface{} `json:"executors"`
        Configs     map[string]ConfigValue `json:"configs"`
        NodeConfigs map[int]string         `json:"node_configs"`
    }

    doc := MarshalDocument{
        Executors:   jd.Executors,
        Configs:     jd.Configs,
        NodeConfigs: jd.NodeConfigs,
    }

    return json.MarshalIndent(doc, "", "  ")
}

func (jd *RawJobConfig) Validate(jc *JobConfig) error {
    var errorMessages []string

    if err := jd.validateExecutors(jc); err != nil {
        errorMessages = append(errorMessages, err.Error())
    }

    if err := jd.validateConfigs(jc); err != nil {
        errorMessages = append(errorMessages, err.Error())
    }

    if err := jd.validateNodeConfigs(jc); err != nil {
        errorMessages = append(errorMessages, err.Error())
    }

    if len(errorMessages) > 0 {
        return errors.New(strings.Join(errorMessages, "\n"))
    }

    return nil
}

func (jd *RawJobConfig) validateExecutors(jc *JobConfig) error {
    var errorMessages []string

    validExecutors := map[string]bool{"local": true, "slurm": true, "temporal": true}

    for executorName, executorConfig := range jd.Executors {
        if !validExecutors[executorName] {
            errorMessages = append(errorMessages, fmt.Sprintf(
                "invalid executor '%s': must be one of 'local', 'slurm', or 'temporal'",
                executorName,
            ))
            continue
        }

        switch executorName {
        case "slurm":
            {
                if err := jd.validateSshConfig(jc, executorConfig, executorName); err != nil {
                    errorMessages = append(errorMessages, err.Error())
                }
            }
        case "local", "temporal":
            {
                configMap, ok := executorConfig.(map[string]any)
                if !ok || (ok && len(configMap) > 0) {
                    errorMessages = append(errorMessages, fmt.Sprintf(
                        "executor '%s' must be an empty dictionary, got: %v",
                        executorName, executorConfig,
                    ))
                }
            }
        }
    }

    if len(errorMessages) > 0 {
        return errors.New(strings.Join(errorMessages, "\n"))
    }
    return nil
}

func (jd *RawJobConfig) validateSshConfig(
    jc *JobConfig, config any, executorName string,
) error {
    configBytes, err := json.Marshal(config)
    if err != nil {
        return fmt.Errorf(
            "failed to marshal SSH config for executor '%s': %w",
            executorName, err,
        )
    }

    var sshConfig SshConfig
    if err := json.Unmarshal(configBytes, &sshConfig); err != nil {
        return fmt.Errorf(
            "invalid SSH config for executor '%s': %w", executorName, err,
        )
    }

    // Validate required fields
    if sshConfig.IpAddr == "" {
        return fmt.Errorf(
            "SSH config for executor '%s' missing required field 'ip_addr'",
            executorName,
        )
    }
    if sshConfig.User == "" {
        return fmt.Errorf(
            "SSH config for executor '%s' missing required field 'user'",
            executorName,
        )
    }
    if sshConfig.TransferAddr == "" {
        return fmt.Errorf(
            "SSH config for executor '%s' missing required field 'transfer_addr'",
            executorName,
        )
    }
    if sshConfig.SchedDir == "" {
        return fmt.Errorf(
            "SSH config for executor '%s' missing required field 'sched_dir'",
            executorName,
        )
    }

    jc.SlurmExecutor = sshConfig
    return nil
}

func (jd *RawJobConfig) validateConfigs(jc *JobConfig) error {
    var errorMessages []string

    for configName, configValue := range jd.Configs {
        // Validate executor reference
        if _, exists := jd.Executors[configValue.Executor]; !exists {
            errorMessages = append(errorMessages, fmt.Sprintf(
                "config '%s' references non-existent executor '%s'",
                configName, configValue.Executor,
            ))
            continue
        }

        // Validate annotations based on executor type
        if err := jd.validateAnnotations(jc, configName, configValue); err != nil {
            errorMessages = append(errorMessages, err.Error())
        }
    }

    if len(errorMessages) > 0 {
        return errors.New(strings.Join(errorMessages, "\n"))
    }
    return nil
}

func validateWalltimeStr(walltime string) error {
    reList := []*regexp.Regexp{
        regexp.MustCompile(`^([0-9]+)$`),
        regexp.MustCompile(`^([0-9]+):([0-5]?[0-9])$`),
        regexp.MustCompile(`^([0-9]+):([0-5]?[0-9]):([0-5]?[0-9])$`),
        regexp.MustCompile(`^([0-9]+)-([0-9]+)$`),
        regexp.MustCompile(`^([0-9]+)-([0-9]+):([0-5]?[0-9])$`),
        regexp.MustCompile(`^([0-9]+)-([0-9]+):([0-5]?[0-9]):([0-5]?[0-9])$`),
    }

    for _, re := range reList {
        if re.MatchString(walltime) {
            return nil
        }
    }
    return fmt.Errorf(
        "invalid time format: %q (expected Slurm walltime); "+
            "acceptable time formats include 'minutes', 'minutes:seconds', "+
            "'hours:minutes:seconds', 'days-hours', 'days-hours:minutes', "+
            "and 'days-hours:minutes:seconds'", walltime,
    )
}

func validateMemStr(memStr string) error {
    memoryRegex := regexp.MustCompile(`^\d+([KMGTP])?$`)
    if !memoryRegex.MatchString(memStr) {
        return fmt.Errorf(
            "invalid memory string %q; expected number followed by optional "+
                "K, M, G, or T suffix (if suffix omitted, units are read "+
                "as MB)", memStr,
        )
    }
    return nil
}

func (jd *RawJobConfig) validateAnnotations(
    jc *JobConfig, configName string, configValue ConfigValue,
) error {
    if configValue.Annotations == nil {
        // Annotations are optional for non-slurm executors
        if configValue.Executor == "local" {
            jc.LocalConfigsByName[configName] = LocalJobConfig{UseDocker: false}
            return nil
        } else if configValue.Executor == "temporal" {
            jc.TemporalConfigsByName[configName] = LocalJobConfig{UseDocker: false}
            return nil
        }
        return fmt.Errorf(
            "config '%s' with slurm executor requires annotations",
            configName,
        )
    }

    // For slurm executor, validate SlurmJobConfig
    annotationsBytes, err := json.Marshal(configValue.Annotations)
    if err != nil {
        return fmt.Errorf(
            "failed to marshal annotations for config '%s': %w",
            configName, err,
        )
    }

    if configValue.Executor == "local" || configValue.Executor == "temporal" {
        // We'll change this later.
        var localConfig LocalJobConfig
        if err := json.Unmarshal(annotationsBytes, &localConfig); err != nil {
            return fmt.Errorf(
                "invalid LocalJobConfig annotations for config '%s': %w",
                configName, err,
            )
        }
        if configValue.Executor == "local" {
            jc.LocalConfigsByName[configName] = localConfig
        } else if configValue.Executor == "temporal" {
            jc.TemporalConfigsByName[configName] = localConfig
        }
        return nil
    }

    var slurmConfig SlurmJobConfig
    if err := json.Unmarshal(annotationsBytes, &slurmConfig); err != nil {
        return fmt.Errorf(
            "invalid SlurmJobConfig annotations for config '%s': %w",
            configName, err,
        )
    }

    if slurmConfig.Partition == nil {
        return fmt.Errorf(
            "invalid SlurmJobConfig annotatins for config '%s'; "+
                "must have 'partition' key", configName,
        )
    }

    if slurmConfig.Mem != nil {
        if err := validateMemStr(*slurmConfig.Mem); err != nil {
            return err
        }
    }

    if slurmConfig.Time != nil {
        if err := validateWalltimeStr(*slurmConfig.Time); err != nil {
            return err
        }
    }

    for key, value := range slurmConfig.Environment {
        if matched, _ := regexp.MatchString(`^[A-Za-z_][A-Za-z0-9_]*$`, key); !matched {
            return fmt.Errorf("invalid Slurm environment variable name %q", key)
        }
        if strings.ContainsAny(value, "\r\n") {
            return fmt.Errorf("Slurm environment variable %q contains a newline", key)
        }
    }

    jc.SlurmConfigsByName[configName] = slurmConfig
    return nil
}

func (jd *RawJobConfig) validateNodeConfigs(jc *JobConfig) error {
    var errorMessages []string

    for nodeID, configName := range jd.NodeConfigs {
        if _, exists := jd.Configs[configName]; !exists {
            errorMessages = append(errorMessages, fmt.Sprintf(
                "node_configs references non-existent config '%s' for node %d",
                configName, nodeID,
            ))
        }

        // Expect this to have already been parsed into jc by this point.
        if config, ok := jc.SlurmConfigsByName[configName]; ok {
            jc.ExecTypeByNode[nodeID] = EXEC_SLURM
            jc.SlurmConfigsByNode[nodeID] = config
        } else if config, ok := jc.LocalConfigsByName[configName]; ok {
            jc.ExecTypeByNode[nodeID] = EXEC_LOCAL
            jc.LocalConfigsByNode[nodeID] = config
        } else if config, ok := jc.TemporalConfigsByName[configName]; ok {
            jc.ExecTypeByNode[nodeID] = EXEC_TEMPORAL
            jc.TemporalConfigsByNode[nodeID] = config
        } else {
            return fmt.Errorf(
                "config name %s exists in raw document but has not been parsed "+
                    "into JobConfig struct", configName,
            )
        }
    }

    if len(errorMessages) > 0 {
        return errors.New(strings.Join(errorMessages, "\n"))
    }
    return nil
}

// Helper function to parse and validate JSON
func ParseAndValidateJobConfig(data []byte) (JobConfig, error) {
    var doc JobConfig
    if err := ParseJobConfig(data, &doc); err != nil {
        return JobConfig{}, fmt.Errorf("failed to parse JSON: %w", err)
    }
    return doc, nil
}

func ParseAndValidateJobConfigFile(file string) (JobConfig, error) {
    fileStream, err := os.ReadFile(file)
    if err != nil {
        return JobConfig{}, err
    }
    return ParseAndValidateJobConfig(fileStream)
}

func GetDefaultConfig(wf Workflow, noTemporal bool) JobConfig {
    var jc JobConfig
    jc.ExecTypeByNode = make(map[int]ExecType)
    if noTemporal {
        jc.LocalConfigsByName = map[string]LocalJobConfig{
            "default": {UseDocker: false},
        }
        jc.LocalConfigsByNode = make(map[int]LocalJobConfig)
        for _, nodeId := range wf.GetNodeIds() {
            jc.LocalConfigsByNode[nodeId] = LocalJobConfig{
                UseDocker: false,
            }
            jc.ExecTypeByNode[nodeId] = EXEC_LOCAL
        }
    } else {
        jc.TemporalConfigsByName = map[string]LocalJobConfig{
            "default": {UseDocker: false},
        }
        jc.TemporalConfigsByNode = make(map[int]LocalJobConfig)
        for _, nodeId := range wf.GetNodeIds() {
            jc.TemporalConfigsByNode[nodeId] = LocalJobConfig{
                UseDocker: false,
            }
            jc.ExecTypeByNode[nodeId] = EXEC_TEMPORAL
        }
    }
    return jc
}

func GetElideableDownloads(
    wf Workflow, nodeId int, idx WorkflowIndex, config JobConfig,
) map[string]struct{} {
    elideableXfers := make(map[string]struct{})
    for _, inLink := range idx.InLinks[nodeId] {
        inPname := inLink.GetSinkPname()
        srcNode := inLink.GetSrcId()
        node, _ := wf.GetNode(nodeId)
        if node.ArgIsInputFile(inPname) {
            if config.ExecTypeByNode[nodeId] == config.ExecTypeByNode[srcNode] {
                elideableXfers[inPname] = struct{}{}
            }
        }
    }
    return elideableXfers
}

func GetElideableUploads(
    wf Workflow, nodeId int, idx WorkflowIndex, config JobConfig,
) map[string]struct{} {
    elideableXfers := make(map[string]struct{})
    for _, outLink := range idx.OutLinks[nodeId] {
        outPname := outLink.GetSinkPname()
        sinkNode := outLink.GetSinkId()
        node, _ := wf.GetNode(nodeId)
        if node.ArgIsOutputFile(outPname) {
            if config.ExecTypeByNode[nodeId] == config.ExecTypeByNode[sinkNode] {
                elideableXfers[outPname] = struct{}{}
            }
        }
    }
    return elideableXfers
}

func RemoveElideableFileXfers(
    cmd *CmdTemplate, wf Workflow, idx WorkflowIndex, config JobConfig,
) {
    elideableDownloads := GetElideableDownloads(wf, cmd.NodeId, idx, config)
    for inFilePname := range cmd.InFiles {
        if _, canElide := elideableDownloads[inFilePname]; canElide {
            delete(cmd.InFiles, inFilePname)
        }
    }
    elideableUploads := GetElideableUploads(wf, cmd.NodeId, idx, config)
    for outFilePname := range cmd.OutFiles {
        if _, canElide := elideableUploads[outFilePname]; canElide {
            delete(cmd.InFiles, outFilePname)
        }
    }

    outFilePnamesRevised := make([]string, 0)
    for _, outFilePname := range cmd.OutFilePnames {
        if _, canElide := elideableUploads[outFilePname]; !canElide {
            outFilePnamesRevised = append(outFilePnamesRevised, outFilePname)
        }
    }
    cmd.OutFilePnames = outFilePnamesRevised
}
