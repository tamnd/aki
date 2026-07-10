// Package structs holds the shared structures more than one type imports
// (spec 2064/f3/19 section 4.1): the open-addressed table, the counted tree,
// and the chunk directory. The types import these, never duplicate them. The
// directory is engine/f3/struct per the doc map; the package is named structs
// because struct is a Go keyword.
package structs
