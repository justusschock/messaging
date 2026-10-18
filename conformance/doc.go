// Package conformance provides a reusable test suite for implementations of
// the messaging Publisher and Subscriber ports.
//
// Transport adapters call [Run] from their own tests. The suite uses real
// time, so it works for adapters that perform network I/O; adapters that are
// purely in-process may also run it inside a testing/synctest bubble.
package conformance
