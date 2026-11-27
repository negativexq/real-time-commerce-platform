// Package procerr ports services.event_processor.errors: an explicit error
// taxonomy whose class decides retry, DLQ, or shutdown.
package procerr

import "errors"

// Kind mirrors the Python exception classes. The class name is recorded in
// DLQ records (error_type), so TypeName() returns the Python name.
type Kind int

const (
	RetryableProcessing Kind = iota
	RetryableDatabase
	MissingBusinessDependency
	FraudContextDependency
	PermanentProcessing
	PermanentDatabaseIntegrity
	FraudEvaluationIntegrity
	AlreadyPersisted
	FatalInfrastructure
	StartupDatabase
	FraudRuleConfiguration
)

var typeNames = map[Kind]string{
	RetryableProcessing:        "RetryableProcessingError",
	RetryableDatabase:          "RetryableDatabaseError",
	MissingBusinessDependency:  "MissingBusinessDependencyError",
	FraudContextDependency:     "FraudContextDependencyError",
	PermanentProcessing:        "PermanentProcessingError",
	PermanentDatabaseIntegrity: "PermanentDatabaseIntegrityError",
	FraudEvaluationIntegrity:   "FraudEvaluationIntegrityError",
	AlreadyPersisted:           "AlreadyPersistedEvent",
	FatalInfrastructure:        "FatalInfrastructureError",
	StartupDatabase:            "StartupDatabaseError",
	FraudRuleConfiguration:     "FraudRuleConfigurationError",
}

// Error is a classified processor error.
type Error struct {
	Kind    Kind
	Message string
	Cause   error
}

func (e *Error) Error() string { return e.Message }
func (e *Error) Unwrap() error { return e.Cause }

// TypeName returns the Python exception class name.
func (e *Error) TypeName() string { return typeNames[e.Kind] }

func New(kind Kind, message string) *Error { return &Error{Kind: kind, Message: message} }

func Wrap(kind Kind, message string, cause error) *Error {
	return &Error{Kind: kind, Message: message, Cause: cause}
}

// As extracts a classified error.
func As(err error) (*Error, bool) {
	var e *Error
	ok := errors.As(err, &e)
	return e, ok
}

// IsRetryable reports RetryableProcessingError or one of its subclasses.
func IsRetryable(err error) bool {
	e, ok := As(err)
	if !ok {
		return false
	}
	switch e.Kind {
	case RetryableProcessing, RetryableDatabase, MissingBusinessDependency, FraudContextDependency:
		return true
	}
	return false
}

// IsPermanent reports PermanentProcessingError or one of its subclasses.
func IsPermanent(err error) bool {
	e, ok := As(err)
	if !ok {
		return false
	}
	switch e.Kind {
	case PermanentProcessing, PermanentDatabaseIntegrity, FraudEvaluationIntegrity:
		return true
	}
	return false
}

// IsMissingDependency reports MissingBusinessDependencyError or a subclass.
func IsMissingDependency(err error) bool {
	e, ok := As(err)
	return ok && (e.Kind == MissingBusinessDependency || e.Kind == FraudContextDependency)
}

// Is reports an exact kind.
func Is(err error, kind Kind) bool {
	e, ok := As(err)
	return ok && e.Kind == kind
}
