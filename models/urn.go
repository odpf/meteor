package models

import "fmt"

func NewURN(service, scope, kind, id string) string {
	return fmt.Sprintf(
		"urn:%s:%s:%s:%s",
		service, scope, kind, id,
	)
}
