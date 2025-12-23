package decoder

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pkg/errors"
)

// DecodedCall represents a decoded function call with its method info and input values.
type DecodedCall struct {
	// Method is the ABI method definition.
	Method *abi.Method `json:"-"`
	// Name is the function name.
	Name string `json:"name"`
	// Selector is the 4-byte function selector.
	Selector [4]byte `json:"selector"`
	// Inputs contains the decoded input parameters as a map of name to value.
	Inputs map[string]interface{} `json:"inputs"`
}

// DecodedOutput represents decoded function return values.
type DecodedOutput struct {
	// Outputs contains the decoded output values.
	Outputs []interface{} `json:"outputs"`
}

// CallDecoder decodes transaction calldata and trace output data.
type CallDecoder struct {
	methods map[[4]byte]abi.Method
}

// NewCallDecoder creates an empty CallDecoder.
func NewCallDecoder() *CallDecoder {
	return &CallDecoder{
		methods: make(map[[4]byte]abi.Method),
	}
}

// NewCallDecoderFromSignatures creates a CallDecoder from a list of function signatures.
// Signatures should be in the format "functionName(type1,type2,...)" e.g. "transfer(address,uint256)".
func NewCallDecoderFromSignatures(signatures []string) (*CallDecoder, error) {
	decoder := NewCallDecoder()
	for _, sig := range signatures {
		if err := decoder.AddSignature(sig); err != nil {
			return nil, errors.Wrapf(err, "failed to add signature: %s", sig)
		}
	}
	return decoder, nil
}

// NewCallDecoderFromABI creates a CallDecoder from a JSON ABI string.
func NewCallDecoderFromABI(abiJSON string) (*CallDecoder, error) {
	parsed, err := abi.JSON(strings.NewReader(abiJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse ABI JSON")
	}

	decoder := NewCallDecoder()
	for _, method := range parsed.Methods {
		var selector [4]byte
		copy(selector[:], method.ID)
		decoder.methods[selector] = method
	}

	return decoder, nil
}

// AddSignature adds a function signature to the decoder.
// Signature format: "functionName(type1,type2,...)" e.g. "transfer(address,uint256)"
func (d *CallDecoder) AddSignature(signature string) error {
	method, err := parseSignature(signature)
	if err != nil {
		return err
	}

	var selector [4]byte
	copy(selector[:], method.ID)
	d.methods[selector] = *method

	return nil
}

// AddMethod adds an ABI method to the decoder.
func (d *CallDecoder) AddMethod(method abi.Method) {
	var selector [4]byte
	copy(selector[:], method.ID)
	d.methods[selector] = method
}

// DecodeInput decodes transaction input data (calldata).
// Returns nil, nil if the function selector is not registered (graceful failure).
func (d *CallDecoder) DecodeInput(data []byte) (*DecodedCall, error) {
	if len(data) < 4 {
		return nil, nil
	}

	var selector [4]byte
	copy(selector[:], data[:4])

	method, ok := d.methods[selector]
	if !ok {
		return nil, nil
	}

	inputs := make(map[string]interface{})
	if len(data) > 4 {
		if err := method.Inputs.UnpackIntoMap(inputs, data[4:]); err != nil {
			return nil, errors.Wrap(err, "failed to unpack input data")
		}
	}

	return &DecodedCall{
		Method:   &method,
		Name:     method.Name,
		Selector: selector,
		Inputs:   inputs,
	}, nil
}

// DecodeInputWithSelector decodes input data when you already know the selector.
// Returns nil, nil if the selector is not registered.
func (d *CallDecoder) DecodeInputWithSelector(selector [4]byte, data []byte) (*DecodedCall, error) {
	method, ok := d.methods[selector]
	if !ok {
		return nil, nil
	}

	inputs := make(map[string]interface{})
	if len(data) > 0 {
		if err := method.Inputs.UnpackIntoMap(inputs, data); err != nil {
			return nil, errors.Wrap(err, "failed to unpack input data")
		}
	}

	return &DecodedCall{
		Method:   &method,
		Name:     method.Name,
		Selector: selector,
		Inputs:   inputs,
	}, nil
}

// DecodeOutput decodes function output/return data using a provided signature.
// The signature should include output types: "functionName(inputs)(outputs)"
// e.g. "balanceOf(address)(uint256)" or just "(uint256)" for outputs only.
func (d *CallDecoder) DecodeOutput(signature string, data []byte) (*DecodedOutput, error) {
	if len(data) == 0 {
		return &DecodedOutput{Outputs: []interface{}{}}, nil
	}

	// Parse output types from signature
	outputTypes, err := parseOutputTypes(signature)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse output types")
	}

	if len(outputTypes) == 0 {
		return &DecodedOutput{Outputs: []interface{}{}}, nil
	}

	// Build arguments for unpacking
	args, err := buildArguments(outputTypes)
	if err != nil {
		return nil, errors.Wrap(err, "failed to build output arguments")
	}

	outputs, err := args.Unpack(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to unpack output data")
	}

	return &DecodedOutput{Outputs: outputs}, nil
}

// GetSelector returns the 4-byte selector for a function signature.
func GetSelector(signature string) [4]byte {
	// Normalize the signature (remove spaces, parameter names)
	normalized := normalizeSignature(signature)
	hash := crypto.Keccak256([]byte(normalized))
	var selector [4]byte
	copy(selector[:], hash[:4])
	return selector
}

// parseSignature parses a function signature and returns an abi.Method.
func parseSignature(signature string) (*abi.Method, error) {
	// Parse function name and parameters
	re := regexp.MustCompile(`^(\w+)\((.*)\)$`)
	matches := re.FindStringSubmatch(signature)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid signature format: %s", signature)
	}

	name := matches[1]
	params := matches[2]

	// Build JSON ABI for this function
	abiJSON, err := buildABIJSON(name, params)
	if err != nil {
		return nil, err
	}

	parsed, err := abi.JSON(strings.NewReader(abiJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse generated ABI")
	}

	method, ok := parsed.Methods[name]
	if !ok {
		return nil, fmt.Errorf("method %s not found in parsed ABI", name)
	}

	return &method, nil
}

// buildABIJSON builds a JSON ABI string for a single function.
func buildABIJSON(name string, params string) (string, error) {
	inputs := []map[string]interface{}{}

	if params != "" {
		types := splitParams(params)
		for i, t := range types {
			input := map[string]interface{}{
				"name": fmt.Sprintf("arg%d", i),
				"type": strings.TrimSpace(t),
			}

			// Handle tuple types (structs)
			if strings.Contains(t, "(") {
				components, baseType, err := parseTupleType(t)
				if err != nil {
					return "", err
				}
				input["type"] = baseType
				input["components"] = components
			}

			inputs = append(inputs, input)
		}
	}

	abiDef := []map[string]interface{}{
		{
			"type":   "function",
			"name":   name,
			"inputs": inputs,
		},
	}

	jsonBytes, err := json.Marshal(abiDef)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal ABI JSON")
	}

	return string(jsonBytes), nil
}

// splitParams splits parameter types, handling nested tuples correctly.
func splitParams(params string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, ch := range params {
		switch ch {
		case '(':
			depth++
			current.WriteRune(ch)
		case ')':
			depth--
			current.WriteRune(ch)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// parseTupleType parses a tuple type like "(uint256,address)[]" into components.
func parseTupleType(t string) ([]map[string]interface{}, string, error) {
	// Find the tuple content
	start := strings.Index(t, "(")
	end := strings.LastIndex(t, ")")

	if start == -1 || end == -1 {
		return nil, "", fmt.Errorf("invalid tuple type: %s", t)
	}

	tupleContent := t[start+1 : end]
	suffix := t[end+1:] // e.g., "[]" for array

	innerTypes := splitParams(tupleContent)
	components := make([]map[string]interface{}, len(innerTypes))

	for i, innerType := range innerTypes {
		comp := map[string]interface{}{
			"name": fmt.Sprintf("field%d", i),
			"type": strings.TrimSpace(innerType),
		}

		// Handle nested tuples
		if strings.Contains(innerType, "(") {
			nested, baseType, err := parseTupleType(innerType)
			if err != nil {
				return nil, "", err
			}
			comp["type"] = baseType
			comp["components"] = nested
		}

		components[i] = comp
	}

	return components, "tuple" + suffix, nil
}

// parseOutputTypes extracts output types from a signature.
// Handles formats like "func(inputs)(outputs)" or just "(outputs)".
func parseOutputTypes(signature string) ([]string, error) {
	// Find the last set of parentheses for outputs
	lastClose := strings.LastIndex(signature, ")")
	if lastClose == -1 {
		return nil, fmt.Errorf("invalid output signature: %s", signature)
	}

	// Find matching open paren
	depth := 0
	lastOpen := -1
	for i := lastClose; i >= 0; i-- {
		if signature[i] == ')' {
			depth++
		} else if signature[i] == '(' {
			depth--
			if depth == 0 {
				lastOpen = i
				break
			}
		}
	}

	if lastOpen == -1 {
		return nil, fmt.Errorf("invalid output signature: %s", signature)
	}

	outputStr := signature[lastOpen+1 : lastClose]
	if outputStr == "" {
		return []string{}, nil
	}

	return splitParams(outputStr), nil
}

// buildArguments creates abi.Arguments from type strings.
func buildArguments(types []string) (abi.Arguments, error) {
	args := make(abi.Arguments, len(types))

	for i, t := range types {
		t = strings.TrimSpace(t)

		// Build a minimal ABI JSON for this type
		abiJSON := fmt.Sprintf(`[{"type":"function","name":"f","outputs":[{"name":"out","type":"%s"}]}]`, t)

		parsed, err := abi.JSON(strings.NewReader(abiJSON))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse type: %s", t)
		}

		method, ok := parsed.Methods["f"]
		if !ok || len(method.Outputs) == 0 {
			return nil, fmt.Errorf("failed to get output for type: %s", t)
		}

		args[i] = method.Outputs[0]
	}

	return args, nil
}

// normalizeSignature removes spaces and parameter names from a signature.
func normalizeSignature(signature string) string {
	// Remove spaces
	sig := strings.ReplaceAll(signature, " ", "")

	// This is a simple normalization - for production use you might want more robust handling
	return sig
}
