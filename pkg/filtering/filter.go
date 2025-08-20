package filtering

import (
	"fmt"
	"regexp"
	"reflect"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go-data-sync-http/pkg/models"
)

// FilterEngine handles field-level and document-level filtering
type FilterEngine struct{}

// NewFilterEngine creates a new filter engine
func NewFilterEngine() *FilterEngine {
	return &FilterEngine{}
}

// ApplyFieldFilter applies field inclusion/exclusion filters to a document
func (fe *FilterEngine) ApplyFieldFilter(doc map[string]interface{}, filter *models.FieldFilter) map[string]interface{} {
	if filter == nil {
		return doc
	}

	filteredDoc := make(map[string]interface{})

	// Always include _id field
	if id, exists := doc["_id"]; exists {
		filteredDoc["_id"] = id
	}

	// Always include specified fields
	if len(filter.AlwaysInclude) > 0 {
		for _, field := range filter.AlwaysInclude {
			if value, exists := doc[field]; exists {
				filteredDoc[field] = value
			}
		}
	}

	if len(filter.IncludeFields) > 0 {
		// Include only specified fields
		for _, field := range filter.IncludeFields {
			if field == "_id" {
				continue // Already included
			}
			if value, exists := doc[field]; exists {
				filteredDoc[field] = value
			}
		}
	} else {
		// Include all fields except excluded ones
		for key, value := range doc {
			excluded := false
			for _, excludeField := range filter.ExcludeFields {
				if key == excludeField {
					excluded = true
					break
				}
			}
			if !excluded {
				filteredDoc[key] = value
			}
		}
	}

	return filteredDoc
}

// BuildDocumentFilterPipeline creates MongoDB aggregation pipeline for document filtering
func (fe *FilterEngine) BuildDocumentFilterPipeline(filter *models.DocumentFilter) []bson.M {
	return fe.buildDocumentFilterPipelineWithPrefix(filter, "")
}

// BuildChangeStreamDocumentFilterPipeline creates MongoDB aggregation pipeline for change stream document filtering
// In change streams, document fields are nested under "fullDocument"
func (fe *FilterEngine) BuildChangeStreamDocumentFilterPipeline(filter *models.DocumentFilter) []bson.M {
	return fe.buildDocumentFilterPipelineWithPrefix(filter, "fullDocument.")
}

// buildDocumentFilterPipelineWithPrefix creates MongoDB aggregation pipeline for document filtering with optional field prefix
func (fe *FilterEngine) buildDocumentFilterPipelineWithPrefix(filter *models.DocumentFilter, fieldPrefix string) []bson.M {
	var pipeline []bson.M

	if filter == nil || len(filter.Criteria) == 0 {
		return pipeline
	}

	matchConditions := bson.M{}

	for _, criteria := range filter.Criteria {
		fieldName := fieldPrefix + criteria.Field
		switch strings.ToLower(criteria.Operator) {
		case "eq":
			matchConditions[fieldName] = criteria.Value
		case "ne":
			matchConditions[fieldName] = bson.M{"$ne": criteria.Value}
		case "in":
			// Validate that criteria.Value is a slice/array for $in operator
			if reflect.TypeOf(criteria.Value).Kind() == reflect.Slice {
				v := reflect.ValueOf(criteria.Value)
				if v.Len() > 0 {
					matchConditions[fieldName] = bson.M{"$in": criteria.Value}
				}
				// If empty slice, skip this condition to avoid MongoDB error
			}
		case "nin":
			// Validate that criteria.Value is a slice/array for $nin operator
			if reflect.TypeOf(criteria.Value).Kind() == reflect.Slice {
				v := reflect.ValueOf(criteria.Value)
				if v.Len() > 0 {
					matchConditions[fieldName] = bson.M{"$nin": criteria.Value}
				}
				// If empty slice, skip this condition to avoid MongoDB error
			}
		case "gt":
			matchConditions[fieldName] = bson.M{"$gt": criteria.Value}
		case "gte":
			matchConditions[fieldName] = bson.M{"$gte": criteria.Value}
		case "lt":
			matchConditions[fieldName] = bson.M{"$lt": criteria.Value}
		case "lte":
			matchConditions[fieldName] = bson.M{"$lte": criteria.Value}
		case "regex":
			matchConditions[fieldName] = bson.M{"$regex": criteria.Value}
		default:
			// Default to equality if operator is not recognized
			matchConditions[fieldName] = criteria.Value
		}
	}

	if len(matchConditions) > 0 {
		matchStage := bson.M{"$match": matchConditions}
		pipeline = append(pipeline, matchStage)
	}

	return pipeline
}

// BuildFieldFilterPipeline creates MongoDB aggregation pipeline for field filtering
func (fe *FilterEngine) BuildFieldFilterPipeline(filter *models.FieldFilter) []bson.M {
	var pipeline []bson.M

	if filter == nil {
		return pipeline
	}

	if len(filter.IncludeFields) > 0 || len(filter.ExcludeFields) > 0 {
		projectStage := bson.M{"$project": bson.M{}}

		// Include specific fields
		if len(filter.IncludeFields) > 0 {
			for _, field := range filter.IncludeFields {
				projectStage["$project"].(bson.M)[field] = 1
			}
		} else {
			// Exclude specific fields
			for _, field := range filter.ExcludeFields {
				projectStage["$project"].(bson.M)[field] = 0
			}
		}

		// Always include fields that should never be excluded
		if len(filter.AlwaysInclude) > 0 {
			for _, field := range filter.AlwaysInclude {
				projectStage["$project"].(bson.M)[field] = 1
			}
		}

		pipeline = append(pipeline, projectStage)
	}

	return pipeline
}

// ValidateDocumentAgainstFilter checks if a document matches the document filter criteria
func (fe *FilterEngine) ValidateDocumentAgainstFilter(doc map[string]interface{}, filter *models.DocumentFilter) bool {
	if filter == nil || len(filter.Criteria) == 0 {
		return true
	}

	for _, criteria := range filter.Criteria {
		value, exists := doc[criteria.Field]
		if !exists {
			return false
		}

		if !fe.evaluateCriteria(value, criteria.Operator, criteria.Value) {
			return false
		}
	}

	return true
}

// evaluateCriteria evaluates a single filter criteria
func (fe *FilterEngine) evaluateCriteria(fieldValue interface{}, operator string, criteriaValue interface{}) bool {
	switch strings.ToLower(operator) {
	case "eq":
		return fe.compareValues(fieldValue, criteriaValue) == 0
	case "ne":
		return fe.compareValues(fieldValue, criteriaValue) != 0
	case "gt":
		return fe.compareValues(fieldValue, criteriaValue) > 0
	case "gte":
		return fe.compareValues(fieldValue, criteriaValue) >= 0
	case "lt":
		return fe.compareValues(fieldValue, criteriaValue) < 0
	case "lte":
		return fe.compareValues(fieldValue, criteriaValue) <= 0
	case "in":
		return fe.valueInSlice(fieldValue, criteriaValue)
	case "nin":
		return !fe.valueInSlice(fieldValue, criteriaValue)
	case "regex":
		return fe.matchesRegex(fieldValue, criteriaValue)
	default:
		return fe.compareValues(fieldValue, criteriaValue) == 0
	}
}

// compareValues compares two values and returns -1, 0, or 1
func (fe *FilterEngine) compareValues(a, b interface{}) int {
	// Handle nil values
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Convert to comparable types
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	// Handle different types
	if aVal.Type() != bVal.Type() {
		// Try to convert to string for comparison
		aStr := fmt.Sprintf("%v", a)
		bStr := fmt.Sprintf("%v", b)
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}

	// Handle specific types
	switch aVal.Kind() {
	case reflect.String:
		aStr := aVal.String()
		bStr := bVal.String()
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		aInt := aVal.Int()
		bInt := bVal.Int()
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		aUint := aVal.Uint()
		bUint := bVal.Uint()
		if aUint < bUint {
			return -1
		} else if aUint > bUint {
			return 1
		}
		return 0
	case reflect.Float32, reflect.Float64:
		aFloat := aVal.Float()
		bFloat := bVal.Float()
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	default:
		// Fallback to string comparison
		aStr := fmt.Sprintf("%v", a)
		bStr := fmt.Sprintf("%v", b)
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}
}

// valueInSlice checks if a value exists in a slice
func (fe *FilterEngine) valueInSlice(value interface{}, slice interface{}) bool {
	sliceVal := reflect.ValueOf(slice)
	if sliceVal.Kind() != reflect.Slice && sliceVal.Kind() != reflect.Array {
		return false
	}

	for i := 0; i < sliceVal.Len(); i++ {
		if fe.compareValues(value, sliceVal.Index(i).Interface()) == 0 {
			return true
		}
	}
	return false
}

// matchesRegex checks if a value matches a regex pattern
func (fe *FilterEngine) matchesRegex(value interface{}, pattern interface{}) bool {
	valueStr := fmt.Sprintf("%v", value)
	patternStr := fmt.Sprintf("%v", pattern)

	regex, err := regexp.Compile(patternStr)
	if err != nil {
		return false
	}

	return regex.MatchString(valueStr)
}