package leaderd

import "errors"

func (l Instance) Validate() error {
	if l.Table == "" {
		return errors.New("required argument table not provided")
	}

	if l.Name == "" {
		return errors.New("required argument name not provided")
	}

	return nil
}
