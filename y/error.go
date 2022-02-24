package y

import (
	"fmt"
	"log"

	"github.com/pkg/errors"
)

var debugMode = true

func Check(err error) {
	if err != nil {
		log.Fatalf("%+v", Wrap(err))
	}
}

// Check2 acts as convenience wrapper around Check, using the 2nd argument as error.
func Check2(_ interface{}, err error) {
	Check(err)
}

// AssertTrue asserts that b is true. Otherwise, it would log fatal.
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}

// AssertTruef is AssertTrue with extra info.
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}

func Wrap(err error) error {
	if !debugMode {
		return err
	}
	return errors.Wrap(err, "")
}

func Wrapf(err error, format string, args ...interface{}) error {
	if !debugMode {
		if err == nil {
			return nil
		}
		return fmt.Errorf(format+"error: %+v", append(args, err)...)
	}
	return errors.Wrapf(err, format, args...)
}
