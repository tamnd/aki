// Package list is the list type (spec 2064/f3/13): the inline listpack-parity
// band and its one-way conversion into the native form. This slice builds the
// inline band in full (a single packed element blob, the point ops over it, and
// the listpack->quicklist promotion at Redis's byte boundary) and leaves the
// native form a placeholder the chunked-deque slice replaces file for file.
package list
