// v0 parse workflow cmds
package parsing

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
)

type PatternQuery struct {
	Root     string
	Pattern  string
	FindFile bool
	FindDir  bool
	Sorted   bool
}

type GlobFunc func(int, string, string, bool, bool) ([]string, error)

type CmdSub struct {
	Start int
	End   int
	Pname string
}

type CmdTemplate struct {
	Version   int
	Id        int
	NodeId    int
	Priority  int
	BaseCmd   []string
	Args      []string
	ImageName string
	Flags     []string
	Envs      map[string]string

	// For iterable vars, we need to keep track of which version
	// of the variable was used to generate a particular command.
	IterVals TypedParams

	// Keep track of input and output files for this command.
	// InFiles are fixed paths, but output files must be stored
	// both as fixed paths (OutFiles) and by paramter names
	// (OutFilePnames), since the filenames of output files may
	// not be known until after runtime (where they may be written
	// to /tmp/output).
	InFiles       map[string][]string
	OutFiles      map[string][]string
	OutDirs       map[string][]string
	OutFilePnames []string

	ResourceReqs ResourceVector
}

type TypedParams struct {
	Bools          map[string]bool
	Ints           map[string]int
	Doubles        map[string]float64
	Strings        map[string]string
	StrLists       map[string][]string
	IntLists       map[string][]int
	DoubleLists    map[string][]float64
	PatternQueries map[string]PatternQuery
	NilVals        map[string]struct{}
}

func concatMapsOfScalars[V any](dst map[string][]V, src map[string]V) {
	for k, v := range src {
		dst[k] = append(dst[k], v)
	}
}

func concatMapsOfLists[V any](dst, src map[string][]V) {
	for k, v := range src {
		dst[k] = append(dst[k], v...)
	}
}

func concatScalarParams(tps []TypedParams) TypedParams {
	newTp := TypedParams{
		StrLists:    make(map[string][]string),
		IntLists:    make(map[string][]int),
		DoubleLists: make(map[string][]float64),
	}

	for _, tp := range tps {
		concatMapsOfScalars(newTp.StrLists, tp.Strings)
		concatMapsOfScalars(newTp.IntLists, tp.Ints)
		concatMapsOfScalars(newTp.DoubleLists, tp.Doubles)
		concatMapsOfLists(newTp.StrLists, tp.StrLists)
		concatMapsOfLists(newTp.IntLists, tp.IntLists)
		concatMapsOfLists(newTp.DoubleLists, tp.DoubleLists)
	}
	return newTp
}

func copyMapOfScalars[K comparable, V any](srcMap map[K]V) map[K]V {
	dstMap := make(map[K]V)
	for k, v := range srcMap {
		dstMap[k] = v
	}
	return dstMap
}

func copyMapOfLists[K comparable, V any](srcMap map[K][]V) map[K][]V {
	dstMap := make(map[K][]V)
	for k, v := range srcMap {
		dstMap[k] = make([]V, len(v))
		copy(dstMap[k], v)
	}
	return dstMap
}

func copyTypedParams(tp TypedParams) TypedParams {
	var newTp TypedParams
	newTp.Bools = copyMapOfScalars(tp.Bools)
	newTp.Ints = copyMapOfScalars(tp.Ints)
	newTp.Doubles = copyMapOfScalars(tp.Doubles)
	newTp.Strings = copyMapOfScalars(tp.Strings)
	newTp.PatternQueries = copyMapOfScalars(tp.PatternQueries)
	newTp.StrLists = copyMapOfLists(tp.StrLists)
	newTp.IntLists = copyMapOfLists(tp.IntLists)
	newTp.DoubleLists = copyMapOfLists(tp.DoubleLists)
	return newTp
}

func copyCmdTemplate(template CmdTemplate) CmdTemplate {
	var newTemplate CmdTemplate
	newTemplate.Id = template.Id
	newTemplate.NodeId = template.NodeId
	newTemplate.ImageName = template.ImageName
	newTemplate.ResourceReqs = template.ResourceReqs
	newTemplate.BaseCmd = make([]string, len(template.BaseCmd))
	newTemplate.Args = make([]string, len(template.Args))
	newTemplate.Flags = make([]string, len(template.Flags))
	newTemplate.InFiles = make(map[string][]string, len(template.InFiles))
	newTemplate.OutFiles = make(map[string][]string, len(template.OutFiles))
	newTemplate.OutFilePnames = make([]string, len(template.OutFilePnames))
	newTemplate.OutDirs = make(map[string][]string, len(template.OutDirs))

	copy(newTemplate.BaseCmd, template.BaseCmd)
	copy(newTemplate.Args, template.Args)
	copy(newTemplate.Flags, template.Flags)
	copy(newTemplate.OutFilePnames, template.OutFilePnames)

	newTemplate.Envs = copyMapOfScalars(template.Envs)
	newTemplate.IterVals = copyTypedParams(template.IterVals)
	newTemplate.InFiles = copyMapOfLists(template.InFiles)
	newTemplate.OutFiles = copyMapOfLists(template.OutFiles)
	newTemplate.OutDirs = copyMapOfLists(template.OutDirs)

	return newTemplate
}

func getRequiredParams(node WorkflowNodeV0) map[string]bool {
	reqParams := make(map[string]bool)
	for _, pname := range node.RequiredParams {
		reqParams[pname] = true
	}
	return reqParams
}

func getIterAttrs(node WorkflowNodeV0, reqParams map[string]bool) map[string]bool {
	if !node.Iterate {
		return map[string]bool{}
	}

	iterAttrs := make(map[string]bool)
	for _, pname := range node.IterAttrs {
		if reqParams[pname] {
			iterAttrs[pname] = true
		}
	}
	return iterAttrs
}

func argTypeIsStr(argType string) bool {
	return argType == "text" || argType == "str" ||
		argType == "directory" || argType == "file" ||
		argType == "dryRunStr"
}

func parsePatternQuery(pqRaw any) (PatternQuery, error) {
	if val, ok := pqRaw.(PatternQuery); ok {
		return val, nil
	}
	pValMap, mapOk := pqRaw.(map[string]any)
	if !mapOk {
		return PatternQuery{}, fmt.Errorf("cannot parse to map")
	}

	root, rootOk := pValMap["root"].(string)
	pattern, patternOk := pValMap["pattern"].(string)
	findFile, findFileOk := pValMap["findFile"].(bool)
	findDir, findDirOk := pValMap["findDir"].(bool)
	if !(rootOk && patternOk && findFileOk && findDirOk) {
		return PatternQuery{}, fmt.Errorf(
			"missing one of root, pattern, findFile, findDir",
		)
	}

	sorted, sortedOk := pValMap["sorted"].(bool)
	if !sortedOk {
		sorted = false
	}

	return PatternQuery{
		Root:     root,
		Pattern:  pattern,
		FindFile: findFile,
		FindDir:  findDir,
		Sorted:   sorted,
	}, nil

}

func (tp *TypedParams) IsNil(pname string) bool {
	_, entryExists := tp.NilVals[pname]
	return entryExists
}

// Identical to LookupParams but returns
// the resolved version of pattern queries rather
// than the unresolved ones.
func (tp *TypedParams) LookupParamParsed(
	pname string, argType string,
) (any, bool) {
	if argType == "patternQuery" {
		val, ok := tp.StrLists[pname]
		return any(val), ok
	}

	return tp.LookupParam(pname, argType)
}

// Lookup param. If it's a pattern query and has been
// resolved, return the resolved version. If it's
// a pattern query that hasn't been resolved, return
// the unresolved version. If it's anything else, return
// the normal value.
func (tp *TypedParams) LookupParamOptionallyParsed(
	pname string, argType string,
) (any, bool) {
	if argType == "patternQuery" {
		val, ok := tp.StrLists[pname]
		if ok {
			return any(val), ok
		}
	}

	return tp.LookupParam(pname, argType)
}

func (tp *TypedParams) LookupParam(
	pname string, argType string,
) (any, bool) {
	if argType == "bool" {
		val, ok := tp.Bools[pname]
		return any(val), ok
	} else if argType == "int" {
		val, ok := tp.Ints[pname]
		return any(val), ok
	} else if argType == "double" {
		val, ok := tp.Doubles[pname]
		return any(val), ok
	} else if argTypeIsStr(argType) {
		val, ok := tp.Strings[pname]
		return any(val), ok
	} else if strings.HasSuffix(argType, "list") {
		listType := strings.Split(argType, " ")[0]
		if argTypeIsStr(listType) {
			val, ok := tp.StrLists[pname]
			return any(val), ok
		} else if listType == "int" {
			val, ok := tp.IntLists[pname]
			return any(val), ok
		} else if listType == "double" {
			val, ok := tp.DoubleLists[pname]
			return any(val), ok
		}
	} else if argType == "patternQuery" {
		val, ok := tp.PatternQueries[pname]
		return any(val), ok
	}

	return nil, false
}

func parseSerializedNumeric[T int | float64](
	propValRaw any,
) (T, error) {
	var emptyT T
	pValDouble, ok := propValRaw.(float64)
	castErr := fmt.Errorf("cannot cast %v to numeric", propValRaw)
	if !ok {
		// The default behavior of BWB seems to be storing int / double lists as JSON
		// lists of strs. This seems counterintuitive, so I want to support both this
		// and the more natural format of using JSON ints / numbers.
		pValStr, strOk := propValRaw.(string)
		if !strOk {
			return emptyT, castErr
		}

		var convErr error
		pValDouble, convErr = strconv.ParseFloat(pValStr, 64)
		if convErr != nil {
			return emptyT, convErr
		}
	}
	return T(pValDouble), nil
}

func parseSerializedNumericList[T int | float64](
	propValRaw []string,
) ([]T, error) {
	ret := make([]T, len(propValRaw))
	for i, v := range propValRaw {
		val, err := parseSerializedNumeric[T](v)
		if err != nil {
			return nil, err
		}
		ret[i] = val
	}

	return ret, nil
}

func (tp *TypedParams) AddSerializedParam(
	propValStr string, pname string, argType string,
) error {
	var propValRaw any
	var err error
	if argTypeIsStr(argType) {
		propValRaw = propValStr
	} else if argType == "int" {
		propValRaw, err = parseSerializedNumeric[int](propValStr)
	} else if argType == "double" {
		propValRaw, err = parseSerializedNumeric[float64](propValStr)
	} else if strings.HasSuffix(argType, "list") {
		lines := strings.Split(propValStr, "\n")
		if len(lines) > 0 && lines[len(lines)-1] == "" {
			lines = lines[:len(lines)-1]
		}

		listType := strings.Split(argType, " ")[0]
		if argTypeIsStr(listType) {
			propValRaw = lines
		} else if listType == "int" {
			propValRaw, err = parseSerializedNumericList[int](lines)
		} else if listType == "double" {
			propValRaw, err = parseSerializedNumericList[float64](lines)
		}
	}

	if err != nil {
		return err
	}

	if err := tp.AddParam(propValRaw, pname, argType); err != nil {
		return err
	}

	return nil
}

func (tp *TypedParams) AddParam(
	propValRaw any, pname string, argType string,
) error {
	if tp.Bools == nil {
		tp.Bools = make(map[string]bool)
	}
	if tp.Ints == nil {
		tp.Ints = make(map[string]int)
	}
	if tp.Doubles == nil {
		tp.Doubles = make(map[string]float64)
	}
	if tp.Strings == nil {
		tp.Strings = make(map[string]string)
	}
	if tp.StrLists == nil {
		tp.StrLists = make(map[string][]string)
	}
	if tp.IntLists == nil {
		tp.IntLists = make(map[string][]int)
	}
	if tp.DoubleLists == nil {
		tp.DoubleLists = map[string][]float64{}
	}
	if tp.PatternQueries == nil {
		tp.PatternQueries = make(map[string]PatternQuery)
	}
	if tp.NilVals == nil {
		tp.NilVals = make(map[string]struct{})
	}

	if propValRaw == nil {
		tp.NilVals[pname] = struct{}{}
		return nil
	}

	castErr := fmt.Errorf("var %s (%v) cannot be cast to %s", pname, propValRaw, argType)
	if argType == "bool" {
		pValBool, ok := propValRaw.(bool)
		if ok {
			tp.Bools[pname] = pValBool
		} else {
			pValNum, ok := propValRaw.(float64)
			if ok {
				tp.Bools[pname] = pValNum > 0
			} else {
				return castErr
			}
		}
	} else if argType == "int" {
		pValDouble, ok := propValRaw.(float64)
		if !ok {
			return castErr
		}
		tp.Ints[pname] = int(pValDouble)
	} else if argType == "double" {
		pValDouble, ok := propValRaw.(float64)
		if !ok {
			return castErr
		}
		tp.Doubles[pname] = pValDouble
	} else if argTypeIsStr(argType) {
		pValStr, ok := propValRaw.(string)
		if !ok {
			return castErr
		}
		tp.Strings[pname] = pValStr
	} else if strings.HasSuffix(argType, "list") {
		var pValList []any
		// Lazy way of coping with the fact that we need to handle
		// []any and []string/float/int equally well.
		switch v := propValRaw.(type) {
		case []any:
			pValList = v
		case []string:
			pValList = make([]any, len(v))
			for i, s := range v {
				pValList[i] = s
			}
		case []int:
			pValList = make([]any, len(v))
			for i, s := range v {
				pValList[i] = s
			}
		case []float64:
			pValList = make([]any, len(v))
			for i, s := range v {
				pValList[i] = s
			}
		default:
			return castErr
		}
		listType := strings.Split(argType, " ")[0]

		if argTypeIsStr(listType) {
			tp.StrLists[pname] = make([]string, len(pValList))
			for i, v := range pValList {
				pValStr, ok := v.(string)
				if !ok {
					return castErr
				}
				tp.StrLists[pname][i] = pValStr
			}
		} else if listType == "int" {
			tp.IntLists[pname] = make([]int, len(pValList))
			for i, v := range pValList {
				pValInt, err := parseSerializedNumeric[int](v)
				if err != nil {
					return fmt.Errorf("error converting pname %s: %s", pname, err)
				}
				tp.IntLists[pname][i] = pValInt
			}
		} else if listType == "double" {
			tp.DoubleLists[pname] = make([]float64, len(pValList))
			for i, v := range pValList {
				pValDouble, err := parseSerializedNumeric[float64](v)
				if err != nil {
					return fmt.Errorf("error converting pname %s: %s", pname, err)
				}
				tp.DoubleLists[pname][i] = pValDouble
			}
		}
	} else if argType == "patternQuery" {
		pq, pqError := parsePatternQuery(propValRaw)
		if pqError != nil {
			return fmt.Errorf(
				"param %s cannot be parsed as pattern query: %s",
				pname, pqError,
			)
		}
		tp.PatternQueries[pname] = pq
	} else {
		return fmt.Errorf("invalid arg type %s", argType)
	}

	delete(tp.NilVals, pname)
	return nil
}

func performPqSubs(pq *PatternQuery, node WorkflowNodeV0, tp TypedParams) error {
	for _, strPtr := range []*string{&pq.Root, &pq.Pattern} {
		subs := getCmdSubs(*strPtr)
		if len(subs) == 0 {
			continue
		}

		var startLocs []int
		for startLoc := range subs {
			startLocs = append(startLocs, startLoc)
		}
		sort.Ints(startLocs)

		previousEnd := 0
		newVal := ""
		for _, startLoc := range startLocs {
			sub := subs[startLoc]

			argType, argTypeExists := node.ArgTypes[sub.Pname]
			if !argTypeExists {
				return fmt.Errorf("pname %s has no argtype", sub.Pname)
			}

			val, ok := tp.LookupParam(sub.Pname, argType.ArgType)
			if !ok {
				return fmt.Errorf("pname %s has no value", sub.Pname)
			}

			newVal += pq.Root[previousEnd:startLoc] + fmt.Sprintf("%v", val)
			previousEnd = sub.End
		}

		*strPtr = newVal
	}

	return nil
}

func (tp *TypedParams) ResolvePq(
	pname string, node WorkflowNodeV0, glob GlobFunc,
) error {
	if _, alreadyResolved := tp.StrLists[pname]; alreadyResolved {
		return nil
	}

	pq, ok := tp.PatternQueries[pname]
	if !ok {
		return fmt.Errorf("pattern query %s does not exist", pname)
	}
	performPqSubs(&pq, node, *tp)

	matches, err := glob(node.Id, pq.Root, pq.Pattern, pq.FindFile, pq.FindDir)
	if err != nil {
		return err
	}

	if pq.Sorted {
		slices.Sort(matches)
	}

	tp.StrLists[pname] = matches
	return nil
}

func parseTypedParams(
	node WorkflowNodeV0, props map[string]any,
) (TypedParams, error) {
	var tp TypedParams
	tp.Bools = make(map[string]bool)
	tp.Ints = make(map[string]int)
	tp.Doubles = make(map[string]float64)
	tp.Strings = make(map[string]string)
	tp.StrLists = make(map[string][]string)
	tp.IntLists = make(map[string][]int)
	tp.DoubleLists = make(map[string][]float64)
	tp.PatternQueries = make(map[string]PatternQuery)

	for pname, argType := range node.ArgTypes {
		propValRaw, ok := props[pname]
		if !ok {
			continue
		}

		if err := tp.AddParam(propValRaw, pname, argType.ArgType); err != nil {
			return tp, fmt.Errorf("error parsing node %d: %s", node.Id, err)
		}
	}

	return tp, nil
}

func strRemoveQuotes(str string) string {
	if strings.HasPrefix(str, "'") && strings.HasSuffix(str, "'") {
		return str[1 : len(str)-1]
	}
	return str
}

func getEnvValStrFromList[T any](list []T) string {
	// Treating the one-element list case as distinct
	// from multi-element lists is annoying, but this behavior
	// is a holdover from BWB.
	if len(list) == 1 {
		return fmt.Sprintf("%v", list[0])
	}

	out := "["
	for i, elRaw := range list {
		out += fmt.Sprintf("\"%v\"", elRaw)

		if i < len(list)-1 {
			out += ","
		}
	}
	out += "]"
	return out
}

func getEnvValStr(pname string, argType string, tp TypedParams) (string, error) {
	notFoundError := fmt.Errorf("param %s not found", pname)
	if argType == "bool" {
		if pVal, ok := tp.Bools[pname]; ok {
			if pVal {
				return "True", nil
			} else {
				return "False", nil
			}
		}
		return "", notFoundError
	} else if argType == "int" {
		if pVal, ok := tp.Ints[pname]; ok {
			return strconv.Itoa(pVal), nil
		}
		return "", notFoundError
	} else if argType == "double" {
		if pVal, ok := tp.Doubles[pname]; ok {
			return strconv.FormatFloat(pVal, 'f', -1, 64), nil
		}
		return "", notFoundError
	} else if argTypeIsStr(argType) {
		if pVal, ok := tp.Strings[pname]; ok {
			return strRemoveQuotes(pVal), nil
		}
		return "", notFoundError
	} else if strings.HasSuffix(argType, "list") {
		listType := strings.Split(argType, " ")[0]

		if argTypeIsStr(listType) {
			if _, ok := tp.StrLists[pname]; !ok {
				return "", notFoundError
			}
			return getEnvValStrFromList(tp.StrLists[pname]), nil
		} else if listType == "int" {
			if _, ok := tp.IntLists[pname]; !ok {
				return "", notFoundError
			}
			return getEnvValStrFromList(tp.IntLists[pname]), nil
		} else if listType == "double" {
			if _, ok := tp.DoubleLists[pname]; !ok {
				return "", notFoundError
			}
			return getEnvValStrFromList(tp.DoubleLists[pname]), nil
		}
	} else if argType == "patternQuery" {
		if _, ok := tp.StrLists[pname]; !ok {
			return "", fmt.Errorf("pattern query %s has not been resolved", pname)
		}
		return getEnvValStrFromList(tp.StrLists[pname]), nil
	}

	return "", fmt.Errorf("unknown argtype %s for %s", argType, pname)
}

// Evaluate non-iterable, non-pattern query env vars, populate template.Envs with
// serialized env var values.
func evaluateEnvVars(
	node WorkflowNodeV0, template *CmdTemplate,
	tp TypedParams, reqParams, iterAttrs map[string]bool,
) error {
	template.Envs = make(map[string]string)
	for pname, argType := range node.ArgTypes {
		if argType.Env == nil {
			continue
		}
		envVar := *argType.Env

		propValRaw, ok := tp.LookupParam(pname, argType.ArgType)
		if !ok || propValRaw == nil {
			if reqParams[pname] {
				return fmt.Errorf(
					"required env var %s of type %s is nil",
					pname, argType.ArgType,
				)
			}
			continue
		}

		if argType.ArgType == "bool" {
			if !ok {
				return fmt.Errorf("could node find node %d var %s", node.Id, pname)
			}

			propValBool, ok := propValRaw.(bool)
			if !ok {
				return fmt.Errorf(
					"could not convert node %d var %s (val %v) to bool",
					node.Id, pname, propValRaw,
				)
			}

			if valStr, err := getEnvValStr(pname, argType.ArgType, tp); err != nil {
				return err
			} else if propValBool {
				template.Envs[envVar] = valStr
			}
		} else {
			if !reqParams[pname] && !node.OptionsChecked[pname] {
				continue
			}

			isIterable := iterAttrs[pname]
			if isIterable {
				continue
			}

			if valStr, err := getEnvValStr(pname, argType.ArgType, tp); err != nil {
				return err
			} else {
				template.Envs[envVar] = valStr
			}
		}

	}
	return nil
}

func joinFlagVal(flag string, val string) string {
	trimmedFlag := strings.TrimSpace(flag)
	trimmedVal := strings.TrimSpace(val)
	if len(trimmedFlag) > 0 && trimmedFlag[len(trimmedFlag)-1] == '=' {
		return fmt.Sprintf("%s%s", trimmedFlag, trimmedVal)
	}
	return fmt.Sprintf("%s %s", trimmedFlag, trimmedVal)
}

func joinFlagValList(flag string, list []string) []string {
	result := make([]string, len(list))
	for i, val := range list {
		result[i] = joinFlagVal(flag, val)
	}
	return result
}

func getScalarArgStr[T any](
	typedProps map[string]T, argType WorkflowArgType,
	pname string, isFlag bool,
) (string, error) {
	if flagVal, ok := typedProps[pname]; ok {
		flagValStr := fmt.Sprintf("%v", flagVal)
		if isFlag {
			return joinFlagVal(*argType.Flag, flagValStr), nil
		}
		return flagValStr, nil
	}

	return "", fmt.Errorf("could not find parameter %s", pname)
}

func getListArgStr[T any](
	typedProps map[string][]T, argType WorkflowArgType,
	pname string, isFlag bool,
) (string, error) {
	flagVal := ""
	if _, ok := typedProps[pname]; ok {
		for i, val := range typedProps[pname] {
			flagVal += fmt.Sprintf("%v", val)
			if i < len(typedProps[pname])-1 {
				flagVal += " "
			}
		}

		if isFlag {
			return joinFlagVal(*argType.Flag, flagVal), nil
		}
		return flagVal, nil
	}

	return "", fmt.Errorf("could not find parameter %s", pname)
}

func getArgStr(pname string, argType WorkflowArgType, tp TypedParams, isFlag bool) (string, error) {
	isPq := argType.ArgType == "patternQuery"
	if isPq {
		if _, ok := tp.StrLists[pname]; !ok {
			return "", fmt.Errorf("unresolved pattern query %s", pname)
		}
	}

	if argType.ArgType == "bool" {
		if tp.Bools[pname] {
			return *argType.Flag, nil
		}
		return "", nil
	}

	if argTypeIsStr(argType.ArgType) {
		return getScalarArgStr(tp.Strings, argType, pname, isFlag)
	}

	if argType.ArgType == "int" {
		return getScalarArgStr(tp.Ints, argType, pname, isFlag)
	}

	if argType.ArgType == "double" {
		return getScalarArgStr(tp.Doubles, argType, pname, isFlag)
	}

	if strings.HasSuffix(argType.ArgType, "list") || isPq {
		flagVal := ""
		listType := strings.Split(argType.ArgType, " ")[0]
		if argTypeIsStr(listType) || isPq {
			return getListArgStr(tp.StrLists, argType, pname, isFlag)
		} else if listType == "int" {
			return getListArgStr(tp.IntLists, argType, pname, isFlag)
		} else if listType == "double" {
			return getListArgStr(tp.DoubleLists, argType, pname, isFlag)
		}

		if isFlag {
			return joinFlagVal(*argType.Flag, flagVal), nil
		}
		return flagVal, nil
	}

	return "", fmt.Errorf("cannot gen flag value for pname %s, type %s", pname, argType.ArgType)
}

func evaluateFlags(
	node WorkflowNodeV0, template *CmdTemplate,
	tp TypedParams, reqParams, iterAttrs,
	varsToIgnore map[string]bool,
) error {
	for pname, argType := range node.ArgTypes {
		if argType.Flag == nil || tp.IsNil(pname) || *argType.Flag == "" {
			continue
		}

		shouldIgnore := varsToIgnore[pname]
		isIterable := iterAttrs[pname]
		if shouldIgnore || isIterable {
			continue
		}

		addParam := false
		addParam = addParam || node.OptionsChecked[pname]
		addParam = addParam || reqParams[pname]
		if argType.ArgType == "bool" {
			addParam = addParam && tp.Bools[pname]
		}

		if addParam {
			flagStr, err := getArgStr(pname, argType, tp, true)
			if err != nil {
				return err
			}
			template.Flags = append(template.Flags, flagStr)
		}
	}
	return nil
}

func evaluateArgs(
	node WorkflowNodeV0, template *CmdTemplate,
	tp TypedParams, reqParams, iterAttrs,
	shouldIgnore map[string]bool,
) error {
	for _, pname := range node.ArgOrder {
		argType := node.ArgTypes[pname]
		if argType.IsArgument == nil || !*argType.IsArgument {
			continue
		}

		if tp.IsNil(pname) {
			if reqParams[pname] {
				return fmt.Errorf(
					"required argument %s of type %s is nil",
					pname, argType.ArgType,
				)
			}

			// CAREFUL: You need to test to verify this doesn't cause issues.
			continue
		}

		// Iterable attrs will be processed later
		if shouldIgnore[pname] || iterAttrs[pname] {
			continue
		}

		argStr, err := getArgStr(pname, argType, tp, false)
		if err != nil {
			return err
		}
		template.Args = append(template.Args, argStr)
	}

	return nil
}

func evaluateInOutFiles(
	node WorkflowNodeV0, template *CmdTemplate,
	tp TypedParams, iterAttrs map[string]bool,
) error {
	template.InFiles = make(map[string][]string)
	template.OutFiles = make(map[string][]string)
	for pname, argType := range node.ArgTypes {
		if argType.InputFile == nil && argType.OutputFile == nil {
			continue
		}

		argIsList := strings.HasSuffix(argType.ArgType, "list")
		baseType := argType.ArgType
		if argIsList {
			baseType = strings.Split(argType.ArgType, " ")[0]
		}

		if !argTypeIsStr(baseType) && argType.ArgType != "patternQuery" {
			return fmt.Errorf(
				"input / output file pname %s is not str or  PQ", pname,
			)
		}

		// Iterble attrs will be processed in evaluteIterables, since
		// the value of an input file may vary during iteration over
		// a list.
		isIterable := iterAttrs[pname]
		isInFile := argType.InputFile != nil && *argType.InputFile && !isIterable
		isOutFile := argType.OutputFile != nil && *argType.OutputFile && !isIterable
		if isInFile || isOutFile {
			var pVal any
			var pValExists bool
			pVal, pValExists = tp.LookupParamParsed(pname, argType.ArgType)
			if !pValExists {
				if isInFile {
					baseErr := "pname %s is listed as input file but is unset"
					if argType.ArgType == "patternQuery" {
						baseErr += "; probably unresolved pattern query"
					}
					return fmt.Errorf(baseErr, pname)
				}

				// Unlike input files, we only store pnames (not vals) for output
				// files, so there's no need to defer evaluation until processing
				// iterables.
				template.OutFilePnames = append(template.OutFilePnames, pname)
				continue
			}

			var convOk bool
			if argIsList || argType.ArgType == "patternQuery" {
				var pValList []string
				pValList, convOk = pVal.([]string)
				if isInFile {
					template.InFiles[pname] = append(
						template.InFiles[pname], pValList...,
					)
				}

				if isOutFile {
					template.OutFiles[pname] = append(
						template.OutFiles[pname], pValList...,
					)
				}
			} else {
				var pValStr string
				pValStr, convOk = pVal.(string)
				if isInFile {
					template.InFiles[pname] = append(
						template.InFiles[pname], pValStr,
					)
				}

				if isOutFile {
					template.OutFiles[pname] = append(
						template.OutFiles[pname], pValStr,
					)
				}
			}
			if !convOk {
				return fmt.Errorf(
					"unable to convert input/output file param %s to str "+
						"or str list", pname,
				)
			}
		}
	}
	return nil
}

/**
 * Parameters:
 *  - list: Some list to be broken up into chunks of group size.
 *  - groupSize: The size of each chunk.
 * Returns:
 *  - slice of serialzied vals, where nth el is serialized nth group of "list".
**/
func getIterableGroups[T any](list []T, groupSize int) [][]T {
	var resultLen int
	if len(list)%groupSize == 0 {
		resultLen = len(list) / groupSize
	} else {
		resultLen = (len(list) / groupSize) + 1
	}

	iterVals := make([][]T, resultLen)
	for i := 0; i < len(list); i += groupSize {
		resultInd := i / groupSize
		group := make([]T, 0, groupSize)

		for j := 0; j < groupSize; j++ {
			if i+j < len(list) {
				group = append(group, list[i+j])
			} else {
				group = append(group, list[len(list)-1])
			}
		}

		iterVals[resultInd] = group
	}

	return iterVals
}

func getIterableGroupStr[T any](groups [][]T, isEnv bool) []string {
	iterStrs := make([]string, len(groups))
	for i, group := range groups {
		if isEnv {
			iterStrs[i] = getEnvValStrFromList(group)
		} else {
			iterStrs[i] = ""
			for j, val := range group {
				iterStrs[i] += fmt.Sprintf("%v", val)
				if j < len(group)-1 {
					iterStrs[i] += " "
				}
			}
		}
	}
	return iterStrs
}

/** Parameters
*   - typedProps: map of node pname to the value corresponding to that pname.
*           As this is an iterable attr, the value will always be a list.
*   - iterTps: A typedParams structure to fill with the
*
**/
func processIterableAttr[T any](
	typedProps map[string][]T, node WorkflowNodeV0, reqParams map[string]bool,
	argType WorkflowArgType, pname string, groupSize int,
	envStrs, argStrs, flagStrs map[string][]string,
) ([][]T, error) {
	addEnv := false
	if argType.Env != nil && *argType.Env != "" {
		addEnv = true
	}

	addArg := false
	addFlag := false
	if argType.IsArgument != nil && *argType.IsArgument {
		addArg = true
	} else if argType.Flag != nil {
		addFlag = addFlag || node.OptionsChecked[pname]
		addFlag = addFlag || reqParams[pname]
	}

	list, ok := typedProps[pname]
	if !ok {
		errBaseStr := "iterable key %s of type %s not found"
		if argType.ArgType == "patternQuery" {
			errBaseStr += "; likely unresolved pattern query"
		}
		return nil, fmt.Errorf(errBaseStr, pname, argType.ArgType)
	}

	if list == nil {
		return [][]T{}, nil
	}

	groups := getIterableGroups(typedProps[pname], groupSize)
	if addEnv {
		envStrs[pname] = getIterableGroupStr(groups, true)
	}

	if addArg {
		argStrs[pname] = getIterableGroupStr(groups, false)
	} else if addFlag {
		flagVal := getIterableGroupStr(groups, false)
		flagStrs[pname] = joinFlagValList(*argType.Flag, flagVal)
	}

	return groups, nil
}

func evaluateIterables(
	node WorkflowNodeV0, template CmdTemplate,
	tp TypedParams, reqParams map[string]bool,
) ([]CmdTemplate, error) {
	if !node.Iterate || len(node.IterAttrs) == 0 {
		return []CmdTemplate{template}, nil
	}

	strGroups := make(map[string][][]string)
	intGroups := make(map[string][][]int)
	doubleGroups := make(map[string][][]float64)

	argStrs := make(map[string][]string)
	flagStrs := make(map[string][]string)
	envStrs := make(map[string][]string)

	maxSize := 0
	groupsPerPname := make(map[string]int)
	for _, pname := range node.IterAttrs {
		// Another weird BWB-ism is that non-required params are not iterated.
		if !reqParams[pname] {
			continue
		}
		if tp.IsNil(pname) {
			return nil, fmt.Errorf("required param %s is nil", pname)
		}

		groupSize, groupSizeExists := node.IterGroupSize[pname]
		if !groupSizeExists {
			return nil, fmt.Errorf("no group size for iterable attr %s", pname)
		}

		argType, ok := node.ArgTypes[pname]
		if !ok {
			return nil, fmt.Errorf("iterable key %s is not a parameter", pname)
		}

		isPq := argType.ArgType == "patternQuery"
		if !isPq && !strings.HasSuffix(argType.ArgType, "list") {
			return nil, fmt.Errorf("iterable key %s is not a list or PQ", pname)
		}

		var err error
		listType := strings.Split(argType.ArgType, " ")[0]
		if argTypeIsStr(listType) || isPq {
			// Note that resolved PQs are stored in tp.StrLists.
			strGroups[pname], err = processIterableAttr(
				tp.StrLists, node, reqParams, argType,
				pname, groupSize, envStrs, argStrs, flagStrs,
			)
			groupsPerPname[pname] = len(strGroups[pname])
		} else if listType == "int" {
			intGroups[pname], err = processIterableAttr(
				tp.IntLists, node, reqParams, argType,
				pname, groupSize, envStrs, argStrs, flagStrs,
			)
			groupsPerPname[pname] = len(intGroups[pname])
		} else if listType == "double" {
			doubleGroups[pname], err = processIterableAttr(
				tp.DoubleLists, node, reqParams, argType,
				pname, groupSize, envStrs, argStrs, flagStrs,
			)
			groupsPerPname[pname] = len(doubleGroups[pname])
		} else {
			return nil, fmt.Errorf("unsupported list type %s for param %s", argType.ArgType, pname)
		}

		if err != nil {
			return nil, fmt.Errorf("error processing attr %s: %s", pname, err)
		}

		maxSize = max(groupsPerPname[pname], maxSize)
	}

	if maxSize == 0 {
		return []CmdTemplate{template}, nil
	}

	ret := make([]CmdTemplate, 0, maxSize)
	for i := 0; i < maxSize; i++ {
		ithIteration := copyCmdTemplate(template)
		for pname := range node.IterGroupSize {
			numGroups := groupsPerPname[pname]

			if envStrList, pnameIsEnv := envStrs[pname]; pnameIsEnv {
				numGroups = len(envStrList)
				if envKey := node.ArgTypes[pname].Env; envKey != nil {
					ithIteration.Envs[*envKey] = envStrList[i%len(envStrList)]
				} else {
					return ret, fmt.Errorf("could not find env key for key %s", pname)
				}
			}

			if argStrList, pnameIsArg := argStrs[pname]; pnameIsArg {
				numGroups = len(argStrList)
				ithIteration.Args = append(
					ithIteration.Args, argStrList[i%len(argStrList)],
				)
			} else if flagStrList, pnameIsFlag := flagStrs[pname]; pnameIsFlag {
				numGroups = len(flagStrList)
				ithIteration.Flags = append(
					ithIteration.Flags, flagStrList[i%len(flagStrList)],
				)
			}

			if numGroups > 0 {
				argType := node.ArgTypes[pname]
				listType := strings.Split(argType.ArgType, " ")[0]
				isPq := argType.ArgType == "patternQuery"
				if argTypeIsStr(listType) || isPq {
					ithIteration.IterVals.StrLists[pname] = strGroups[pname][i%numGroups]
				} else if node.ArgTypes[pname].ArgType == "int" {
					ithIteration.IterVals.IntLists[pname] = intGroups[pname][i%numGroups]
				} else if node.ArgTypes[pname].ArgType == "double" {
					ithIteration.IterVals.DoubleLists[pname] = doubleGroups[pname][i%numGroups]
				}

				// Validation ensures that only string types can be
				// input files.
				if argType.InputFile != nil && *argType.InputFile {
					ithIteration.InFiles[pname] = append(
						ithIteration.InFiles[pname],
						strGroups[pname][i%numGroups]...,
					)
				}

				if argType.OutputFile != nil && *argType.OutputFile {
					ithIteration.OutFiles[pname] = append(
						ithIteration.OutFiles[pname],
						strGroups[pname][i%numGroups]...,
					)
				}
			}
		}

		ret = append(ret, ithIteration)
	}
	return ret, nil
}

// Return map of starting location of each substitution to
// location. Doing it as a map lets us easily iterate over
// substitutions in order when building cmd.
func getCmdSubs(cmd string) map[int]CmdSub {
	// Regex for strings of form "_bwb{VAR_NAME}"
	ret := make(map[int]CmdSub)
	r := regexp.MustCompile(`_bwb\{([^\}]*)\}`)
	subStrMatches := r.FindAllStringSubmatchIndex(cmd, -1)

	for _, match := range subStrMatches {
		ret[match[0]] = CmdSub{
			// start and end give start and end positions of _bwb{...}
			// in command.
			Start: match[0],
			End:   match[1],
			// pname is whatever is inside _bwb{...} brackets
			Pname: cmd[match[2]:match[3]],
		}
	}

	return ret
}
func performCmdSubs(
	node WorkflowNodeV0, template *CmdTemplate, nonIterVals TypedParams,
	iterAttrs map[string]bool, skipIters bool,
) (map[string]bool, error) {
	revisedCmds := make([]string, len(node.Command))
	subbedVars := make(map[string]bool)
	for i, cmdClause := range node.Command {
		revisedCmds[i] = ""
		cmdSubs := getCmdSubs(cmdClause)

		var startLocs []int
		for startLoc := range cmdSubs {
			startLocs = append(startLocs, startLoc)
		}
		sort.Ints(startLocs)

		previousEnd := 0
		for _, startLoc := range startLocs {
			pname := cmdSubs[startLoc].Pname
			subIdStr := fmt.Sprintf(
				"node %d, cmd %d, pos %d-%d",
				node.Id, i, startLoc, cmdSubs[startLoc].End,
			)

			pnameIsIterable := iterAttrs[pname]
			if pnameIsIterable && skipIters {
				continue
			}

			if argType, validPname := node.ArgTypes[pname]; validPname {
				subbedVars[pname] = true
				var serializedPval string
				var serializationErr error

				if pnameIsIterable {
					serializedPval, serializationErr = getArgStr(
						pname, argType, template.IterVals, false,
					)
				} else {
					serializedPval, serializationErr = getArgStr(
						pname, argType, nonIterVals, false,
					)
				}

				if serializationErr != nil {
					return nil, fmt.Errorf("error substituting %s: %s", subIdStr, serializationErr)
				}
				revisedCmds[i] += cmdClause[previousEnd:startLoc] + serializedPval
			} else {
				return nil, fmt.Errorf(
					"%s has invalid substitution variable %s", subIdStr, pname,
				)
			}

			previousEnd = cmdSubs[startLoc].End
		}

		revisedCmds[i] += cmdClause[previousEnd:]
	}

	template.BaseCmd = revisedCmds
	return subbedVars, nil
}

func ParseNodeCmdV0(node WorkflowNodeV0, tp TypedParams) ([]CmdTemplate, error) {
	nodeId := node.Id
	var template CmdTemplate
	template.NodeId = node.Id
	template.BaseCmd = node.Command
	template.ImageName = fmt.Sprintf("%s:%s", node.ImageName, node.ImageTag)
	template.ResourceReqs = node.ResourceReqs

	reqParams := getRequiredParams(node)
	iterAttrs := getIterAttrs(node, reqParams)

	subbedVars, err := performCmdSubs(node, &template, tp, iterAttrs, true)
	if err != nil {
		return nil, fmt.Errorf(
			"error parsing node %d substitutions: %s",
			nodeId, err,
		)
	}

	err = evaluateEnvVars(node, &template, tp, reqParams, iterAttrs)
	if err != nil {
		return nil, fmt.Errorf("error parsing node %d env vars: %s", nodeId, err)
	}

	err = evaluateFlags(node, &template, tp, reqParams, iterAttrs, subbedVars)
	if err != nil {
		return nil, fmt.Errorf("error parsing node %d flags: %s", nodeId, err)
	}

	err = evaluateArgs(node, &template, tp, reqParams, iterAttrs, subbedVars)
	if err != nil {
		return nil, fmt.Errorf("error parsing node %d args: %s", nodeId, err)
	}

	err = evaluateInOutFiles(node, &template, tp, iterAttrs)
	if err != nil {
		return nil, fmt.Errorf(
			"error parsing node %d input / output files: %s",
			nodeId, err,
		)
	}

	var iterations []CmdTemplate
	iterations, err = evaluateIterables(node, template, tp, reqParams)
	if err != nil {
		return nil, fmt.Errorf(
			"error parsing node %d iterables: %s",
			nodeId, err,
		)
	}

	for i := range iterations {
		_, err := performCmdSubs(node, &iterations[i], tp, iterAttrs, false)
		if err != nil {
			return nil, fmt.Errorf(
				"error parsing node %d, iteration %d substitutions: %s",
				nodeId, i, err,
			)
		}
	}

	return iterations, nil
}

func CmdToStr(template CmdTemplate) string {
	out := ""
	for envK, envV := range template.Envs {
		out += fmt.Sprintf("%s=%s ", envK, envV)
	}

	out += strings.Join(template.BaseCmd, " && ") + " "
	out += strings.Join(template.Flags, " ") + " "
	out += strings.Join(template.Args, " ") + " "

	return strings.ReplaceAll(out, "$", "\\$")
}

func shellQuoteArg(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func FormSingularityCmdPrefix(
	template CmdTemplate,
	volumes map[string]string,
	useGpu bool,
	sifPath string,
) string {
	gpuFlag := ""
	if useGpu {
		gpuFlag = "--nv"
	}

	cntPaths := make([]string, 0, len(volumes))
	for k := range volumes {
		cntPaths = append(cntPaths, k)
	}
	// Sort volumes by length of host path. Singularity requires
	// that if one volume's host path is a prefix of another,
	// the shorter one comes first.
	sort.Slice(cntPaths, func(i, j int) bool {
		return len(volumes[cntPaths[i]]) < len(volumes[cntPaths[j]])
	})

	volumesStr := ""
	for _, cntPath := range cntPaths {
		hostPath := volumes[cntPath]
		volumesStr += fmt.Sprintf("-B %s:%s ", hostPath, cntPath)
	}

	return fmt.Sprintf(
		"singularity exec %s -p -i --containall --cleanenv %s %s",
		gpuFlag, volumesStr, sifPath,
	)
}

func FormSingularityCmd(
	template CmdTemplate,
	volumes map[string]string,
	sifPath string,
	useGpu bool,
) (string, []string) {
	envStrs := make([]string, 0)
	for envK, envV := range template.Envs {
		envStrs = append(
			envStrs, fmt.Sprintf("SINGULARITYENV_%s=%s", envK, envV),
		)
	}

	cmdStr := strings.Join(template.BaseCmd, " && ") + " "
	cmdStr += strings.Join(template.Flags, " ") + " "
	cmdStr += strings.Join(template.Args, " ")
	//cmdStr = strings.ReplaceAll(cmdStr, "$", "\\$")

	singularityPrefix := FormSingularityCmdPrefix(template, volumes, useGpu, sifPath)
	fullCmd := fmt.Sprintf("%s sh -c %s", singularityPrefix, shellQuoteArg(cmdStr))

	return fullCmd, envStrs
}

func FormDockerCmdPrefix(
	template CmdTemplate,
	volumes map[string]string,
	useGpu bool,
	cntName string,
) string {
	envStr := ""
	for envK, envV := range template.Envs {
		trimmedVal := strings.TrimSpace(envV)
		if strings.Contains(trimmedVal, " ") {
			envStr += fmt.Sprintf("-e %s=\"%s\" ", envK, envV)
		} else {
			envStr += fmt.Sprintf("-e %s=%s ", envK, envV)
		}
	}

	gpuFlag := ""
	if useGpu {
		gpuFlag = "--gpus all"
	}

	volumesStr := ""
	for cntPath, hostPath := range volumes {
		volumesStr += fmt.Sprintf("-v %s:%s ", hostPath, cntPath)
	}

	return fmt.Sprintf(
		"docker run --rm --name %s %s %s %s %s ",
		cntName, gpuFlag, envStr, volumesStr, template.ImageName,
	)
}

func FormDockerCmd(
	template CmdTemplate,
	volumes map[string]string,
	useGpu bool,
	cntName string,
) (string, []string) {
	cmdStr := strings.Join(template.BaseCmd, " && ") + " "
	cmdStr += strings.Join(template.Flags, " ") + " "
	cmdStr += strings.Join(template.Args, " ")

	dockerPrefix := FormDockerCmdPrefix(template, volumes, useGpu, cntName)
	fullCmd := fmt.Sprintf(
		"%s sh -c '%s'", dockerPrefix, cmdStr,
	)

	return fullCmd, []string{}
}

func DryRun(workflow WorkflowV0) ([]string, error) {
	inLinks, _, err := getInAndOutLinks(&workflow)
	if err != nil {
		return nil, err
	}

	var cmdStrs []string
	topSort, err := topSort(&workflow)
	if err != nil {
		return nil, fmt.Errorf("failed top sort: %s", err)
	}

	if _, err := ParseAndValidateWorkflow(&workflow); err != nil {
		return nil, fmt.Errorf("failed building index: %s", err)
	}

	for _, nodeId := range topSort {
		node := workflow.Nodes[nodeId]
		baseProps := workflow.NodeBaseProps[nodeId]
		revisedNode := copyWorkflowNode(node)
		revisedProps := copyMapOfScalars(baseProps)
		reqParams := getRequiredParams(node)

		for pname := range workflow.Nodes[nodeId].ArgTypes {
			_, pnameIsIterable := node.IterGroupSize[pname]

			// Replace all incoming links with strings indicating their origin
			// node and origin channel. These strings will be treated as the values
			// of those paramters, so the paramter parsing code can otherwise work
			// as normal.
			inLink, hasIncomingLink := inLinks[nodeId][pname]
			if hasIncomingLink {
				if pnameIsIterable {
					revisedProps[pname] = fmt.Sprintf(
						"ITERABLE{%d.%s}", inLink.GetSrcId(), inLink.GetSrcPname(),
					)
				} else {
					revisedProps[pname] = fmt.Sprintf(
						"{%d.%s}", inLink.GetSrcId(), inLink.GetSrcPname(),
					)
				}
				argTypeEntry := revisedNode.ArgTypes[pname]
				argTypeEntry.ArgType = "dryRunStr"
				revisedNode.ArgTypes[pname] = argTypeEntry
			}
			revisedNode.IterAttrs = make([]string, 0)

		}

		// Verify that all properties have corresponding argTypes.
		for pname := range baseProps {
			if _, pnameHasArgType := node.ArgTypes[pname]; !pnameHasArgType {
				return nil, fmt.Errorf(
					"error parsing node %d parameters: attr %s has no arg type entry",
					nodeId, pname,
				)
			}

			if node.ArgTypes[pname].ArgType == "patternQuery" {
				pq, pqErr := parsePatternQuery(baseProps[pname])
				if pqErr != nil {
					return nil, fmt.Errorf(
						"node %d, var %s cannot be parsed as pattern query: %s",
						node.Id, pname, pqErr,
					)
				}

				if pq.FindDir && pq.FindFile {
					revisedProps[pname] = fmt.Sprintf(
						"GLOB_MATCH{%s/%s}", pq.Root, pq.Pattern,
					)
				} else if pq.FindDir {
					revisedProps[pname] = fmt.Sprintf(
						"GLOB_DIRS_MATCH{%s/%s}", pq.Root, pq.Pattern,
					)
				} else if pq.FindFile {
					revisedProps[pname] = fmt.Sprintf(
						"GLOB_FILE_MATCH{%s/%s}", pq.Root, pq.Pattern,
					)
				}

				argTypeEntry := revisedNode.ArgTypes[pname]
				argTypeEntry.ArgType = "dryRunStr"
				revisedNode.ArgTypes[pname] = argTypeEntry
			}

			if reqParams[pname] && revisedProps[pname] == nil {
				return nil, fmt.Errorf(
					"required parameter %s of node %d is nil",
					pname, nodeId,
				)
			}

		}

		tp, err := parseTypedParams(revisedNode, revisedProps)
		if err != nil {
			return nil, fmt.Errorf(
				"unable to parsed typed params of node %d: %s",
				nodeId, err,
			)
		}

		// There are no pattern queries to parse (we converted all PQ argtypes
		// to str lists), so we can safely pass a DummyFS which returns nothing
		// when queried.
		cmdTemplates, cmdErrs := ParseNodeCmdV0(revisedNode, tp)
		if cmdErrs != nil {
			return nil, fmt.Errorf("error parsing cmds: %s", cmdErrs)
		}

		for _, template := range cmdTemplates {
			cmdStrs = append(cmdStrs, CmdToStr(template))
		}
	}

	return cmdStrs, nil
}

func validateWorkflow(workflow WorkflowV0) error {
	// dstNode -> dstPname -> srcNode
	idToNode := make(map[int]WorkflowNodeV0)
	for _, node := range workflow.Nodes {
		idToNode[node.Id] = node
	}

	// Verify that link values names correspond to actual nodes parameters.
	// Populate inLinks map for easy lookup of incoming links.
	inLinks, _, err := getInAndOutLinks(&workflow)
	if err != nil {
		return err
	}

	for nodeId, node := range workflow.Nodes {
		baseProps, basePropsExist := workflow.NodeBaseProps[nodeId]
		if !basePropsExist {
			return fmt.Errorf("no base props entry for node %d", nodeId)
		}
		reqParams := getRequiredParams(node)

		// Verify that all properties have corresponding argTypes.
		for pname := range baseProps {
			if _, pnameHasArgType := node.ArgTypes[pname]; !pnameHasArgType {
				return fmt.Errorf(
					"error parsing node %d parameters: attr %s has no arg type entry",
					nodeId, pname,
				)
			}

			// Verify that required params have non-null values,
			// unless there is an incoming link to this node that
			// will set them to a non-null value during workflow
			// runtime.
			if reqParams[pname] {
				_, hasIncomingLink := inLinks[nodeId][pname]
				if baseProps[pname] == nil && !hasIncomingLink {
					return fmt.Errorf(
						"required parameter %s of node %d is nil",
						pname, nodeId,
					)
				}
			}

		}

		// Verify that all node params listed as iterable have iterable type
		// (list or patternQuery) and that they have corresponding values /
		// argTypes.
		for iterKey := range node.IterGroupSize {
			if _, iterKeyHasProp := baseProps[iterKey]; !iterKeyHasProp {
				return fmt.Errorf(
					"iterable key %s of node %d has no corresponding parameter",
					iterKey, nodeId,
				)
			}

			if _, iterKeyHasArgType := node.ArgTypes[iterKey]; iterKeyHasArgType {
				argType := node.ArgTypes[iterKey].ArgType
				if !strings.HasSuffix(argType, "list") && argType != "patternQuery" {
					return fmt.Errorf(
						"iterable key %s of node %d has non-list arg type %s",
						iterKey, nodeId, argType,
					)
				}
			} else {
				return fmt.Errorf(
					"iterable key %s of node %d has no corresponding arg type",
					iterKey, nodeId,
				)
			}
		}

		// Verify that there are no type errors inside the node's parameter values.
		_, paramTypeErr := parseTypedParams(node, baseProps)
		if paramTypeErr != nil {
			return fmt.Errorf(
				"error parsing node %d parameters: %s",
				nodeId, paramTypeErr,
			)
		}

		// Make sure all substitutions using _bwb{...} in command body refer
		// to valid variables.
		// Clauses of cmd will be conjoined as [clause1] && [clause2] && ...
		for cmdInd, cmdClause := range node.Command {
			cmdSubKeys := getCmdSubs(cmdClause)
			for _, cmdSub := range cmdSubKeys {
				if _, validPname := baseProps[cmdSub.Pname]; !validPname {
					errFmt := "substitution for non-existent var %s at " +
						"positions %d-%d of cmd %d of node %d"
					return fmt.Errorf(
						errFmt, cmdSub.Pname, cmdSub.Start, cmdSub.End,
						cmdInd, nodeId,
					)
				}
			}
		}
	}

	return nil
}
