package httpx

import "testing"

func TestIsJSONContentType(t *testing.T) {
	t.Parallel()

	for value, want := range map[string]bool{
		"application/json":                true,
		"application/json; charset=utf-8": true,
		"application/problem+json":        true,
		"application/octet-stream":        false,
		"not a content type":              false,
		"":                                false,
	} {
		if got := IsJSONContentType(value); got != want {
			t.Errorf("IsJSONContentType(%q) = %v, want %v", value, got, want)
		}
	}
}
