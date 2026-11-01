package httpx

import (
	"mime"
	"strings"
)

// IsJSONContentType reports whether value is application/json or application/*+json.
func IsJSONContentType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && (mediaType == "application/json" || strings.HasSuffix(mediaType, "+json"))
}
