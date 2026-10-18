// Package memory provides a bounded, process-local implementation of the
// messaging Publisher and Subscriber ports.
//
// [Broker] is a reference transport for tests, local composition and
// in-process event fan-out. It does not persist messages.
package memory
