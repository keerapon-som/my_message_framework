package utils

import "reflect"

// GetStructName returns the name of the struct type
func GetStructName(v interface{}) string {
	t := reflect.TypeOf(v)

	// If it's a pointer, get the underlying element
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Check if it's a struct
	if t.Kind() != reflect.Struct {
		return ""
	}

	return t.Name()
}
