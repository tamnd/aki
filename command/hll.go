package command

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/tamnd/aki/keyspace"
)

// HyperLogLog. An HLL is stored as a Redis string of type string holding a 16
// byte header (HYLL magic, encoding byte, three reserved bytes, an 8 byte
// cardinality cache) followed by a DENSE or SPARSE register body. The register
// derivation, the DENSE bit packing, the SPARSE opcodes and the cardinality
// estimator all match Redis so a blob written by one is readable by the other.
//
// Two places follow real Redis 7.x rather than the spec text in doc 13. The
// register index is the low 14 bits of the hash and the count is the run of
// trailing zeros, the same as hyperloglog.c, not the high-bit form the doc
// sketches. The estimator is the tau/sigma function from Ertl 2017 that Redis
// ships, not the loglog-beta polynomial the doc quotes, so PFCOUNT returns the
// same integer Redis would.

const (
	hllP         = 14
	hllRegisters = 1 << hllP // 16384
	hllQ         = 64 - hllP // 50
	hllHdrSize   = 16
	hllDenseSize = hllRegisters * 6 / 8 // 12288

	hllDense  = 0
	hllSparse = 1

	// hllSparseMaxBytes is the default body size past which a SPARSE HLL is
	// promoted to DENSE. It matches Redis's hll-sparse-max-bytes default. The live
	// value comes from the hll-sparse-max-bytes directive at PFADD time; this is
	// the fallback when there is no dispatcher to read it from.
	hllSparseMaxBytes = 3000

	// hllSparseValMax is the largest register value a VAL opcode can hold. A run
	// that needs a larger value forces a DENSE encoding.
	hllSparseValMax = 32

	// hllAlphaInf is the bias constant Redis uses in the tau/sigma estimator. It
	// is 1/(2*ln 2), the asymptotic alpha of the harmonic-mean HLL estimator.
	hllAlphaInf = 0.7213475204444817

	hllCardValidBit = uint64(1) << 63
)

const (
	hllNotValidError = "WRONGTYPE Key is not a valid HyperLogLog string value."
	hllNoKeyError    = "ERR The specified key does not exist"
)

// hllCommands returns the HyperLogLog command group.
func hllCommands() []*CmdDesc {
	return []*CmdDesc{
		{Name: "pfadd", Group: GroupHyperLogLog, Since: "2.8.9",
			Arity: -2, Flags: FlagWrite | FlagDenyOOM | FlagFast, FirstKey: 1, LastKey: 1, Step: 1,
			Handler: handlePFAdd},
		{Name: "pfcount", Group: GroupHyperLogLog, Since: "2.8.9",
			Arity: -2, Flags: FlagReadOnly, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: handlePFCount},
		{Name: "pfmerge", Group: GroupHyperLogLog, Since: "2.8.9",
			Arity: -2, Flags: FlagWrite | FlagDenyOOM, FirstKey: 1, LastKey: -1, Step: 1,
			Handler: handlePFMerge},
		{Name: "pfdebug", Group: GroupHyperLogLog, Since: "2.8.9",
			Arity: -3, Flags: FlagWrite | FlagAdmin, FirstKey: 2, LastKey: 2, Step: 1,
			Handler: handlePFDebug},
		{Name: "pfselftest", Group: GroupHyperLogLog, Since: "2.8.9",
			Arity: 1, Flags: FlagAdmin, Handler: handlePFSelfTest},
	}
}

// murmurHash64A is the 64-bit MurmurHash variant Redis hashes HLL elements with.
// The seed and the mixing constants match hyperloglog.c so the same element maps
// to the same register in both servers.
func murmurHash64A(data []byte, seed uint64) uint64 {
	const m = 0xc6a4a7935bd1e995
	const r = 47
	h := seed ^ (uint64(len(data)) * m)

	n := len(data) &^ 7
	for i := 0; i < n; i += 8 {
		k := binary.LittleEndian.Uint64(data[i : i+8])
		k *= m
		k ^= k >> r
		k *= m
		h ^= k
		h *= m
	}

	tail := data[n:]
	switch len(tail) {
	case 7:
		h ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		h ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		h ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		h ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		h ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		h ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		h ^= uint64(tail[0])
		h *= m
	}

	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}

// hllPatLen maps an element to its register index and the count it would write.
// The index is the low 14 bits of the hash; the count is one plus the number of
// trailing zeros in the remaining 50 bits, with a sentinel so the count never
// exceeds 51.
func hllPatLen(ele []byte) (index int, count byte) {
	hash := murmurHash64A(ele, 0xadc83b19)
	index = int(hash & (hllRegisters - 1))
	hash >>= hllP
	hash |= uint64(1) << hllQ
	var bit uint64 = 1
	count = 1
	for hash&bit == 0 {
		count++
		bit <<= 1
	}
	return
}

// denseGet reads the 6-bit register at index i from a DENSE body.
func denseGet(b []byte, i int) byte {
	byteIndex := i * 6 / 8
	shift := uint(i * 6 % 8)
	v := uint16(b[byteIndex]) >> shift
	if shift > 2 {
		v |= uint16(b[byteIndex+1]) << (8 - shift)
	}
	return byte(v & 0x3F)
}

// denseSet writes val into the 6-bit register at index i of a DENSE body.
func denseSet(b []byte, i int, val byte) {
	byteIndex := i * 6 / 8
	shift := uint(i * 6 % 8)
	b[byteIndex] = (b[byteIndex] &^ (0x3F << shift)) | ((val & 0x3F) << shift)
	if shift > 2 {
		b[byteIndex+1] = (b[byteIndex+1] &^ (0x3F >> (8 - shift))) | ((val & 0x3F) >> (8 - shift))
	}
}

// hllIsValid reports whether blob looks like an HLL string: the HYLL magic and a
// known encoding byte.
func hllIsValid(blob []byte) bool {
	return len(blob) >= hllHdrSize &&
		blob[0] == 'H' && blob[1] == 'Y' && blob[2] == 'L' && blob[3] == 'L' &&
		(blob[4] == hllDense || blob[4] == hllSparse)
}

// hllReadRegisters decodes a validated HLL blob into a flat register array. It
// returns an error only when the body is truncated.
func hllReadRegisters(blob []byte) (*[hllRegisters]byte, error) {
	var regs [hllRegisters]byte
	body := blob[hllHdrSize:]
	if blob[4] == hllDense {
		if len(body) < hllDenseSize {
			return nil, errors.New("dense body too short")
		}
		for i := range hllRegisters {
			regs[i] = denseGet(body, i)
		}
		return &regs, nil
	}
	reg := 0
	for i := 0; i < len(body); {
		b := body[i]
		switch {
		case b&0x80 != 0: // VAL
			val := ((b >> 2) & 0x1F) + 1
			run := int(b&0x03) + 1
			for j := 0; j < run && reg < hllRegisters; j++ {
				regs[reg] = val
				reg++
			}
			i++
		case b&0x40 != 0: // XZERO
			if i+1 >= len(body) {
				return nil, errors.New("truncated XZERO opcode")
			}
			reg += (int(b&0x3F)<<8 | int(body[i+1])) + 1
			i += 2
		default: // ZERO
			reg += int(b&0x3F) + 1
			i++
		}
	}
	return &regs, nil
}

// hllDenseFromRegisters packs a register array into a DENSE blob with an
// invalidated cardinality cache.
func hllDenseFromRegisters(regs *[hllRegisters]byte) []byte {
	blob := make([]byte, hllHdrSize+hllDenseSize)
	copy(blob, "HYLL")
	blob[4] = hllDense
	body := blob[hllHdrSize:]
	for i := range hllRegisters {
		if regs[i] != 0 {
			denseSet(body, i, regs[i])
		}
	}
	hllInvalidateCache(blob)
	return blob
}

// hllSparseFromRegisters encodes a register array into a canonical SPARSE blob.
// It reports ok=false when the result cannot be SPARSE: a register value above
// the VAL ceiling, or a body past the size threshold. The caller falls back to
// DENSE in that case.
func hllSparseFromRegisters(regs *[hllRegisters]byte, maxBytes int) ([]byte, bool) {
	var body []byte
	i := 0
	for i < hllRegisters {
		v := regs[i]
		if v == 0 {
			j := i
			for j < hllRegisters && regs[j] == 0 {
				j++
			}
			run := j - i
			for run > 0 {
				chunk := min(run, hllRegisters)
				if chunk <= 64 {
					body = append(body, byte(chunk-1)) // ZERO
				} else {
					body = append(body, 0x40|byte((chunk-1)>>8), byte((chunk-1)&0xFF)) // XZERO
				}
				run -= chunk
			}
			i = j
			continue
		}
		if v > hllSparseValMax {
			return nil, false
		}
		j := i
		for j < hllRegisters && regs[j] == v {
			j++
		}
		run := j - i
		for run > 0 {
			chunk := min(run, 4)
			body = append(body, 0x80|((v-1)<<2)|byte(chunk-1)) // VAL
			run -= chunk
		}
		i = j
	}
	if len(body) > maxBytes {
		return nil, false
	}
	blob := make([]byte, hllHdrSize+len(body))
	copy(blob, "HYLL")
	blob[4] = hllSparse
	copy(blob[hllHdrSize:], body)
	hllInvalidateCache(blob)
	return blob, true
}

// hllEncode re-encodes a register array, staying SPARSE when asked and able,
// otherwise DENSE.
func hllEncode(regs *[hllRegisters]byte, preferSparse bool, maxBytes int) []byte {
	if preferSparse {
		if blob, ok := hllSparseFromRegisters(regs, maxBytes); ok {
			return blob
		}
	}
	return hllDenseFromRegisters(regs)
}

func hllInvalidateCache(blob []byte) {
	c := binary.LittleEndian.Uint64(blob[8:16])
	binary.LittleEndian.PutUint64(blob[8:16], c|hllCardValidBit)
}

// hllSigma is the sigma correction from Ertl 2017, transcribed from
// hyperloglog.c.
func hllSigma(x float64) float64 {
	if x == 1.0 {
		return math.Inf(1)
	}
	y := 1.0
	z := x
	for {
		x = x * x
		zPrime := z
		z += x * y
		y += y
		if zPrime == z {
			break
		}
	}
	return z
}

// hllTau is the tau correction from Ertl 2017, transcribed from hyperloglog.c.
func hllTau(x float64) float64 {
	if x == 0.0 || x == 1.0 {
		return 0.0
	}
	y := 1.0
	z := 1.0 - x
	for {
		x = math.Sqrt(x)
		zPrime := z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y
		if zPrime == z {
			break
		}
	}
	return z / 3
}

// hllCount estimates the cardinality of a register array. It is the exact
// tau/sigma estimator Redis 7.x uses, so the result matches Redis.
func hllCount(regs *[hllRegisters]byte) uint64 {
	var reghisto [64]int
	for i := range hllRegisters {
		reghisto[regs[i]]++
	}
	m := float64(hllRegisters)
	z := m * hllTau((m-float64(reghisto[hllQ+1]))/m)
	for j := hllQ; j >= 1; j-- {
		z += float64(reghisto[j])
		z *= 0.5
	}
	z += m * hllSigma(float64(reghisto[0])/m)
	return uint64(math.Round(hllAlphaInf * m * m / z))
}

// handlePFAdd implements PFADD key [element ...]. It returns 1 when the register
// representation changed, which includes creating the key.
func handlePFAdd(ctx *Ctx) {
	key := ctx.Argv[1]
	elements := ctx.Argv[2:]
	maxBytes := int(ctx.d.confInt("hll-sparse-max-bytes", hllSparseMaxBytes))
	var (
		wrongTyp bool
		notHLL   bool
		ret      int64
	)
	done := ctx.updateShard(key, func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if found && hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		var regs *[hllRegisters]byte
		preferSparse := true
		changed := false
		if found {
			if !hllIsValid(body) {
				notHLL = true
				return nil
			}
			regs, err = hllReadRegisters(body)
			if err != nil {
				notHLL = true
				return nil
			}
			preferSparse = body[4] == hllSparse
		} else {
			regs = &[hllRegisters]byte{}
			changed = true // creation is a change
		}
		for _, e := range elements {
			idx, cnt := hllPatLen(e)
			if regs[idx] < cnt {
				regs[idx] = cnt
				changed = true
			}
		}
		if !changed {
			return nil
		}
		blob := hllEncode(regs, preferSparse, maxBytes)
		ret = 1
		return db.Set(key, blob, keyspace.TypeString, keyspace.EncRaw, keepTTL(hdr, found))
	})
	if !done {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notHLL:
		ctx.enc().WriteError(hllNotValidError)
	default:
		ctx.enc().WriteInteger(ret)
	}
}

// handlePFCount implements PFCOUNT key [key ...]. A single key estimates that
// key; several keys estimate the union without modifying any source. A missing
// key contributes nothing, so PFCOUNT of only missing keys returns 0.
func handlePFCount(ctx *Ctx) {
	keys := ctx.Argv[1:]
	var (
		wrongTyp bool
		notHLL   bool
		result   uint64
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		var merged [hllRegisters]byte
		for _, key := range keys {
			body, hdr, found, err := db.Get(key)
			if err != nil {
				return err
			}
			if !found {
				continue
			}
			if hdr.Type != keyspace.TypeString {
				wrongTyp = true
				return nil
			}
			if !hllIsValid(body) {
				notHLL = true
				return nil
			}
			regs, err := hllReadRegisters(body)
			if err != nil {
				notHLL = true
				return nil
			}
			for i := range hllRegisters {
				if regs[i] > merged[i] {
					merged[i] = regs[i]
				}
			}
		}
		result = hllCount(&merged)
		return nil
	})
	if !ok {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notHLL:
		ctx.enc().WriteError(hllNotValidError)
	default:
		ctx.enc().WriteInteger(int64(result))
	}
}

// handlePFMerge implements PFMERGE dest [source ...]. The destination is folded
// into the union of itself and the sources and rewritten as a DENSE HLL, keeping
// its own TTL.
func handlePFMerge(ctx *Ctx) {
	dest := ctx.Argv[1]
	var (
		wrongTyp bool
		notHLL   bool
	)
	done := ctx.update(func(db *keyspace.DB) error {
		var merged [hllRegisters]byte
		var destTTL int64 = -1
		for idx, key := range ctx.Argv[1:] {
			body, hdr, found, err := db.Get(key)
			if err != nil {
				return err
			}
			if found {
				if hdr.Type != keyspace.TypeString {
					wrongTyp = true
					return nil
				}
				if !hllIsValid(body) {
					notHLL = true
					return nil
				}
				regs, err := hllReadRegisters(body)
				if err != nil {
					notHLL = true
					return nil
				}
				for i := range hllRegisters {
					if regs[i] > merged[i] {
						merged[i] = regs[i]
					}
				}
			}
			if idx == 0 {
				destTTL = keepTTL(hdr, found)
			}
		}
		blob := hllDenseFromRegisters(&merged)
		return db.Set(dest, blob, keyspace.TypeString, keyspace.EncRaw, destTTL)
	})
	if !done {
		return
	}
	switch {
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notHLL:
		ctx.enc().WriteError(hllNotValidError)
	default:
		ctx.enc().WriteStatus("OK")
	}
}

// handlePFDebug implements the admin inspection command PFDEBUG. The output of
// these subcommands is not part of the stable wire surface.
func handlePFDebug(ctx *Ctx) {
	sub := strings.ToUpper(string(ctx.Argv[1]))
	key := ctx.Argv[2]
	switch sub {
	case "GETREG":
		pfDebugGetReg(ctx, key)
	case "DECODE":
		pfDebugDecode(ctx, key)
	case "ENCODING":
		pfDebugEncoding(ctx, key)
	case "TODENSE":
		pfDebugToDense(ctx, key)
	default:
		ctx.enc().WriteError("ERR Unknown PFDEBUG subcommand or wrong number of arguments")
	}
}

func pfDebugGetReg(ctx *Ctx, key []byte) {
	var (
		wrongTyp, notHLL, missing bool
		regs                      *[hllRegisters]byte
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if !found {
			missing = true
			return nil
		}
		if hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		if !hllIsValid(body) {
			notHLL = true
			return nil
		}
		regs, err = hllReadRegisters(body)
		if err != nil {
			notHLL = true
		}
		return nil
	})
	if !ok {
		return
	}
	switch {
	case missing:
		ctx.enc().WriteError(hllNoKeyError)
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notHLL:
		ctx.enc().WriteError(hllNotValidError)
	default:
		enc := ctx.enc()
		enc.WriteArrayLen(hllRegisters)
		for i := range hllRegisters {
			enc.WriteInteger(int64(regs[i]))
		}
	}
}

func pfDebugDecode(ctx *Ctx, key []byte) {
	var (
		wrongTyp, notHLL, missing, isDense bool
		out                                string
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if !found {
			missing = true
			return nil
		}
		if hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		if !hllIsValid(body) {
			notHLL = true
			return nil
		}
		if body[4] == hllDense {
			isDense = true
			return nil
		}
		out = hllDecodeSparse(body[hllHdrSize:])
		return nil
	})
	if !ok {
		return
	}
	switch {
	case missing:
		ctx.enc().WriteError(hllNoKeyError)
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notHLL:
		ctx.enc().WriteError(hllNotValidError)
	case isDense:
		ctx.enc().WriteError("ERR HLL encoding is not sparse")
	default:
		ctx.enc().WriteBulkStringStr(out)
	}
}

// hllDecodeSparse renders a SPARSE body as one opcode per line.
func hllDecodeSparse(body []byte) string {
	var b strings.Builder
	for i := 0; i < len(body); {
		op := body[i]
		switch {
		case op&0x80 != 0:
			val := int((op>>2)&0x1F) + 1
			run := int(op&0x03) + 1
			b.WriteString("VAL val=")
			b.WriteString(strconv.Itoa(val))
			b.WriteString(" len=")
			b.WriteString(strconv.Itoa(run))
			b.WriteByte('\n')
			i++
		case op&0x40 != 0:
			if i+1 >= len(body) {
				return b.String()
			}
			run := (int(op&0x3F)<<8 | int(body[i+1])) + 1
			b.WriteString("XZERO len=")
			b.WriteString(strconv.Itoa(run))
			b.WriteByte('\n')
			i += 2
		default:
			run := int(op&0x3F) + 1
			b.WriteString("ZERO len=")
			b.WriteString(strconv.Itoa(run))
			b.WriteByte('\n')
			i++
		}
	}
	return b.String()
}

func pfDebugEncoding(ctx *Ctx, key []byte) {
	var (
		wrongTyp, notHLL, missing bool
		name                      string
	)
	ok := ctx.view(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if !found {
			missing = true
			return nil
		}
		if hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		if !hllIsValid(body) {
			notHLL = true
			return nil
		}
		if body[4] == hllDense {
			name = "dense"
		} else {
			name = "sparse"
		}
		return nil
	})
	if !ok {
		return
	}
	switch {
	case missing:
		ctx.enc().WriteError(hllNoKeyError)
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notHLL:
		ctx.enc().WriteError(hllNotValidError)
	default:
		ctx.enc().WriteStatus(name)
	}
}

func pfDebugToDense(ctx *Ctx, key []byte) {
	var (
		wrongTyp, notHLL, missing bool
	)
	done := ctx.update(func(db *keyspace.DB) error {
		body, hdr, found, err := db.Get(key)
		if err != nil {
			return err
		}
		if !found {
			missing = true
			return nil
		}
		if hdr.Type != keyspace.TypeString {
			wrongTyp = true
			return nil
		}
		if !hllIsValid(body) {
			notHLL = true
			return nil
		}
		if body[4] == hllDense {
			return nil // already dense, no write needed
		}
		regs, err := hllReadRegisters(body)
		if err != nil {
			notHLL = true
			return nil
		}
		blob := hllDenseFromRegisters(regs)
		return db.Set(key, blob, keyspace.TypeString, keyspace.EncRaw, keepTTL(hdr, found))
	})
	if !done {
		return
	}
	switch {
	case missing:
		ctx.enc().WriteError(hllNoKeyError)
	case wrongTyp:
		ctx.enc().WriteError(wrongTypeError)
	case notHLL:
		ctx.enc().WriteError(hllNotValidError)
	default:
		ctx.enc().WriteStatus("OK")
	}
}

// handlePFSelfTest runs the built-in HLL checks: DENSE pack round-trips, SPARSE
// opcode round-trips, and a cardinality estimate within the expected bound. It
// answers OK or an error describing the first failure.
func handlePFSelfTest(ctx *Ctx) {
	if err := hllSelfTest(); err != nil {
		ctx.enc().WriteError("ERR " + err.Error())
		return
	}
	ctx.enc().WriteStatus("OK")
}

func hllSelfTest() error {
	// DENSE pack/unpack round-trips for every index at a few values.
	body := make([]byte, hllDenseSize)
	for i := range hllRegisters {
		val := byte(i % 64)
		denseSet(body, i, val)
		if got := denseGet(body, i); got != val {
			return errors.New("dense register round-trip failed")
		}
		denseSet(body, i, 0)
	}

	// SPARSE encode then decode round-trips the register values.
	var regs [hllRegisters]byte
	regs[5] = 3
	regs[10] = 7
	regs[20000%hllRegisters] = 12
	blob, ok := hllSparseFromRegisters(&regs, hllSparseMaxBytes)
	if !ok {
		return errors.New("sparse encode failed")
	}
	back, err := hllReadRegisters(blob)
	if err != nil {
		return err
	}
	if *back != regs {
		return errors.New("sparse register round-trip failed")
	}

	// Cardinality of a known synthetic workload is within 5% of the truth.
	var probe [hllRegisters]byte
	for i := range 1000 {
		idx, cnt := hllPatLen([]byte("element:" + strconv.Itoa(i)))
		if probe[idx] < cnt {
			probe[idx] = cnt
		}
	}
	est := float64(hllCount(&probe))
	if est < 950 || est > 1050 {
		return errors.New("cardinality estimate out of bounds")
	}
	return nil
}
