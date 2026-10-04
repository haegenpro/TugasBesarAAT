package clients

import (
	"errors"
	"fmt"
	"math"
)

var ErrContractViolation = errors.New("source contract violation")

// requireFields checks the fields the Aggregator's mapping contract needs.
// Other source fields remain optional and are retained as attributes.
func requireFields(record map[string]any, fields ...string) error {
	for _, field := range fields {
		if value, found := record[field]; !found || value == nil {
			return fmt.Errorf("%w: missing required field %q", ErrContractViolation, field)
		}
	}
	return nil
}

func requiredString(record map[string]any, field string) (string, error) {
	value, ok := record[field].(string)
	if !ok || value == "" {
		return "", fmt.Errorf("%w: missing or invalid required field %q", ErrContractViolation, field)
	}
	return value, nil
}

func requiredNumber(record map[string]any, field string) (float64, error) {
	value, ok := record[field].(float64)
	if !ok {
		return 0, fmt.Errorf("%w: missing or invalid required field %q", ErrContractViolation, field)
	}
	return value, nil
}

func requiredInt(record map[string]any, field string) (int, error) {
	value, err := requiredNumber(record, field)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || math.Trunc(value) != value || value < 0 || value > math.MaxInt32 {
		return 0, fmt.Errorf("%w: invalid nonnegative integer field %q", ErrContractViolation, field)
	}
	return int(value), nil
}

// unmappedFields keeps source fields without a top-level canonical mapping.
// Required and explicitly mapped source fields are excluded by name.
func unmappedFields(record map[string]any, mapped ...string) map[string]any {
	known := make(map[string]bool, len(mapped))
	for _, field := range mapped {
		known[field] = true
	}
	attributes := make(map[string]any)
	for field, value := range record {
		if !known[field] && value != nil {
			attributes[field] = value
		}
	}
	return attributes
}
