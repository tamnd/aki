// Package obs1srv is the serving layer of the obs1 driver: the RESP
// listeners, dispatch onto the engine, the node mesh, and the emulated
// cluster protocol (spec 2064/obs1 docs 07 and 11). It sits above
// engine/obs1 the way f3srv sits above engine/f3, and shares its import
// boundary: obs1 packages and the standard library, nothing else.
//
// Empty at O0a; the first serving code lands with the O1 write path.
package obs1srv
