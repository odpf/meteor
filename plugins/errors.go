package plugins

import "fmt"

// InvalidConfigError is returned when a plugin's configuration is invalid.
type InvalidConfigError struct {
	Type PluginType
}

func (err InvalidConfigError) Error() string {
	return fmt.Sprintf("invalid %s config", err.Type)
}

// NotFoundError contains fields required to checks for a missing plugin.
type NotFoundError struct {
	Type PluginType
	Name string
}

func (err NotFoundError) Error() string {
	return fmt.Sprintf("could not find %s \"%s\"", err.Type, err.Name)
}

type RetryError struct {
	Err error
}

func (e RetryError) Error() string {
	return e.Err.Error()
}

func (e RetryError) Unwrap() error {
	return e.Err
}

func (e RetryError) Is(target error) bool {
	_, ok := target.(RetryError)
	return ok
}

func NewRetryError(err error) error {
	if err == nil {
		return nil
	}
	return RetryError{Err: err}
}
