// Package sqlo1a is the Track A store backend of the sqlo1 driver: the
// engine/sqlo1 runtime over a real SQLite file (spec 2064/sqlo1 doc 02).
// It exists to answer a question with numbers, whether SQLite plus a hot
// tier can carry the aki performance bar, and it lives or dies by the A3
// decision milestone.
//
// Empty at S0; the first code lands with the A2 store slices.
package sqlo1a
