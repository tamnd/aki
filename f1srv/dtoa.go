package f1srv

import (
	"math"
	"strconv"
)

// formatScore renders a float64 the way Redis renders a sorted-set score, appending the
// bytes to dst and returning the extended slice. Byte-for-byte parity with Redis matters:
// ZSCORE/ZADD INCR/ZMSCORE/ZINCRBY and the withscores range replies all echo the score as a
// bulk string, and compatibility suites compare those bytes against a reference Redis.
//
// This is a direct port of d2string in Redis util.c, which is a two-tier scheme:
//
//   - nan, +inf, -inf, and signed zero are spelled out explicitly (libc spellings vary).
//   - an integer-valued double inside +/- LLONG_MAX/2 is printed as a plain decimal
//     integer via the long path (double2ll then ll2string). This is why 2e18 comes out
//     "2000000000000000000" (fixed) even though it is a large magnitude.
//   - everything else goes through fpconv_dtoa, Loitsch's grisu2 shortest round-trip
//     formatter, which chooses fixed vs scientific presentation from the digit string and
//     decimal exponent. This is why 9e18 comes out "9e+18": it is above LLONG_MAX/2, so it
//     misses the integer path and grisu2 prints it in scientific form.
//
// Applying a presentation rule to Go's own strconv shortest digits cannot reproduce this,
// so the grisu2 emitter is ported verbatim below.
func formatScore(dst []byte, value float64) []byte {
	if math.IsNaN(value) {
		return append(dst, "nan"...)
	}
	if math.IsInf(value, 0) {
		if value < 0 {
			return append(dst, "-inf"...)
		}
		return append(dst, "inf"...)
	}
	if value == 0 {
		// Signed zero: 1.0/-0.0 is -inf, 1.0/+0.0 is +inf.
		if math.Signbit(value) {
			return append(dst, "-0"...)
		}
		return append(dst, '0')
	}
	if ll, ok := double2ll(value); ok {
		return strconv.AppendInt(dst, ll, 10)
	}
	return fpconvDtoa(dst, value)
}

// double2ll reports whether value is integer-valued and safely inside the range where a cast
// to int64 loses nothing, and if so returns that int64. It ports double2ll from util.c: the
// guard rejects magnitudes above LLONG_MAX/2 (above 2^52 every representable double is already
// integer-valued, so the interesting rejections are the large ones), then the round-trip
// ll == value proves there was no fractional part.
func double2ll(value float64) (int64, bool) {
	const half = float64(math.MaxInt64 / 2)
	if value < -half || value > half {
		return 0, false
	}
	ll := int64(value)
	if float64(ll) == value {
		return ll, true
	}
	return 0, false
}

// The remainder is a faithful port of deps/fpconv/fpconv_dtoa.c (grisu2) and its cached
// powers of ten, so f1srv's non-integer scores match Redis digit-for-digit.

const (
	dtoaFracmask = 0x000FFFFFFFFFFFFF
	dtoaExpmask  = 0x7FF0000000000000
	dtoaHidden   = 0x0010000000000000
	dtoaSignmask = 0x8000000000000000
	dtoaExpbias  = 1023 + 52
)

type dtoaFp struct {
	frac uint64
	exp  int
}

var dtoaTens = [20]uint64{
	10000000000000000000, 1000000000000000000, 100000000000000000, 10000000000000000,
	1000000000000000, 100000000000000, 10000000000000, 1000000000000,
	100000000000, 10000000000, 1000000000, 100000000,
	10000000, 1000000, 100000, 10000,
	1000, 100, 10, 1,
}

func dtoaBuildFp(d float64) dtoaFp {
	bits := math.Float64bits(d)
	var fp dtoaFp
	fp.frac = bits & dtoaFracmask
	fp.exp = int((bits & dtoaExpmask) >> 52)
	if fp.exp != 0 {
		fp.frac += dtoaHidden
		fp.exp -= dtoaExpbias
	} else {
		fp.exp = -dtoaExpbias + 1
	}
	return fp
}

func dtoaNormalize(fp *dtoaFp) {
	for fp.frac&dtoaHidden == 0 {
		fp.frac <<= 1
		fp.exp--
	}
	shift := 64 - 52 - 1
	fp.frac <<= uint(shift)
	fp.exp -= shift
}

func dtoaBoundaries(fp *dtoaFp, lower, upper *dtoaFp) {
	upper.frac = (fp.frac << 1) + 1
	upper.exp = fp.exp - 1
	for upper.frac&(dtoaHidden<<1) == 0 {
		upper.frac <<= 1
		upper.exp--
	}
	uShift := 64 - 52 - 2
	upper.frac <<= uint(uShift)
	upper.exp = upper.exp - uShift

	lShift := 1
	if fp.frac == dtoaHidden {
		lShift = 2
	}
	lower.frac = (fp.frac << uint(lShift)) - 1
	lower.exp = fp.exp - lShift

	lower.frac <<= uint(lower.exp - upper.exp)
	lower.exp = upper.exp
}

func dtoaMultiply(a, b *dtoaFp) dtoaFp {
	const lomask = 0x00000000FFFFFFFF
	ahBl := (a.frac >> 32) * (b.frac & lomask)
	alBh := (a.frac & lomask) * (b.frac >> 32)
	alBl := (a.frac & lomask) * (b.frac & lomask)
	ahBh := (a.frac >> 32) * (b.frac >> 32)

	tmp := (ahBl & lomask) + (alBh & lomask) + (alBl >> 32)
	tmp += 1 << 31 // round up
	return dtoaFp{
		frac: ahBh + (ahBl >> 32) + (alBh >> 32) + (tmp >> 32),
		exp:  a.exp + b.exp + 64,
	}
}

func dtoaRoundDigit(digits []byte, ndigits int, delta, rem, kappa, frac uint64) {
	for rem < frac && delta-rem >= kappa &&
		(rem+kappa < frac || frac-rem > rem+kappa-frac) {
		digits[ndigits-1]--
		rem += kappa
	}
}

func dtoaGenerateDigits(fp, upper, lower *dtoaFp, digits []byte, k *int) int {
	wfrac := upper.frac - fp.frac
	delta := upper.frac - lower.frac

	var one dtoaFp
	one.frac = uint64(1) << uint(-upper.exp)
	one.exp = upper.exp

	part1 := upper.frac >> uint(-one.exp)
	part2 := upper.frac & (one.frac - 1)

	idx, kappa := 0, 10
	// divp walks dtoaTens starting at index 10 (1000000000).
	for divIdx := 10; kappa > 0; divIdx++ {
		div := dtoaTens[divIdx]
		digit := part1 / div
		if digit != 0 || idx != 0 {
			digits[idx] = byte(digit) + '0'
			idx++
		}
		part1 -= digit * div
		kappa--

		tmp := (part1 << uint(-one.exp)) + part2
		if tmp <= delta {
			*k += kappa
			dtoaRoundDigit(digits, idx, delta, tmp, div<<uint(-one.exp), wfrac)
			return idx
		}
	}

	// unit walks dtoaTens from index 18 (10) downward.
	unitIdx := 18
	for {
		part2 *= 10
		delta *= 10
		kappa--

		digit := part2 >> uint(-one.exp)
		if digit != 0 || idx != 0 {
			digits[idx] = byte(digit) + '0'
			idx++
		}
		part2 &= one.frac - 1
		if part2 < delta {
			*k += kappa
			dtoaRoundDigit(digits, idx, delta, part2, one.frac, wfrac*dtoaTens[unitIdx])
			return idx
		}
		unitIdx--
	}
}

func dtoaGrisu2(d float64, digits []byte, k *int) int {
	w := dtoaBuildFp(d)

	var lower, upper dtoaFp
	dtoaBoundaries(&w, &lower, &upper)

	dtoaNormalize(&w)

	var kk int
	cp := dtoaFindCachedPow10(upper.exp, &kk)

	w = dtoaMultiply(&w, &cp)
	upper = dtoaMultiply(&upper, &cp)
	lower = dtoaMultiply(&lower, &cp)

	lower.frac++
	upper.frac--

	*k = -kk
	return dtoaGenerateDigits(&w, &upper, &lower, digits, k)
}

func dtoaAbs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// dtoaEmitDigits ports emit_digits: given the significant digits and the decimal exponent K,
// it picks plain-integer, fixed-decimal, or scientific presentation and appends the result.
func dtoaEmitDigits(dst []byte, digits []byte, ndigits int, k int, neg bool) []byte {
	exp := dtoaAbs(k + ndigits - 1)

	// write plain integer
	if k >= 0 && exp < ndigits+7 {
		dst = append(dst, digits[:ndigits]...)
		for i := 0; i < k; i++ {
			dst = append(dst, '0')
		}
		return dst
	}

	// write decimal without scientific notation
	if k < 0 && (k > -7 || exp < 4) {
		offset := ndigits - dtoaAbs(k)
		if offset <= 0 {
			offset = -offset
			dst = append(dst, '0', '.')
			for i := 0; i < offset; i++ {
				dst = append(dst, '0')
			}
			dst = append(dst, digits[:ndigits]...)
			return dst
		}
		dst = append(dst, digits[:offset]...)
		dst = append(dst, '.')
		dst = append(dst, digits[offset:ndigits]...)
		return dst
	}

	// write decimal with scientific notation
	bound := 18
	if neg {
		bound = 17
	}
	if ndigits > bound {
		ndigits = bound
	}

	dst = append(dst, digits[0])
	if ndigits > 1 {
		dst = append(dst, '.')
		dst = append(dst, digits[1:ndigits]...)
	}
	dst = append(dst, 'e')
	if k+ndigits-1 < 0 {
		dst = append(dst, '-')
	} else {
		dst = append(dst, '+')
	}

	cent := 0
	if exp > 99 {
		cent = exp / 100
		dst = append(dst, byte(cent)+'0')
		exp -= cent * 100
	}
	if exp > 9 {
		dec := exp / 10
		dst = append(dst, byte(dec)+'0')
		exp -= dec * 10
	} else if cent != 0 {
		dst = append(dst, '0')
	}
	dst = append(dst, byte(exp%10)+'0')
	return dst
}

// fpconvDtoa is the grisu2 entry point: it handles the sign, then emits the shortest digits.
// The special-case filtering (nan/inf/zero) is done by formatScore before this is reached, so
// here d is always finite and nonzero, but the sign handling is kept faithful.
func fpconvDtoa(dst []byte, d float64) []byte {
	neg := false
	if math.Float64bits(d)&dtoaSignmask != 0 {
		dst = append(dst, '-')
		neg = true
	}

	var digits [18]byte
	var k int
	ndigits := dtoaGrisu2(d, digits[:], &k)
	return dtoaEmitDigits(dst, digits[:], ndigits, k, neg)
}

// dtoaFindCachedPow10 ports find_cachedpow10 over the precomputed powers-of-ten table.
func dtoaFindCachedPow10(exp int, k *int) dtoaFp {
	const (
		npowers    = 87
		steppowers = 8
		firstpower = -348
		expmax     = -32
		expmin     = -60
		oneLogTen  = 0.30102999566398114
	)
	approx := int(-float64(exp+npowers) * oneLogTen)
	idx := (approx - firstpower) / steppowers
	for {
		current := exp + dtoaPowersTen[idx].exp + 64
		if current < expmin {
			idx++
			continue
		}
		if current > expmax {
			idx--
			continue
		}
		*k = firstpower + idx*steppowers
		return dtoaPowersTen[idx]
	}
}

var dtoaPowersTen = [87]dtoaFp{
	{18054884314459144840, -1220}, {13451937075301367670, -1193},
	{10022474136428063862, -1166}, {14934650266808366570, -1140},
	{11127181549972568877, -1113}, {16580792590934885855, -1087},
	{12353653155963782858, -1060}, {18408377700990114895, -1034},
	{13715310171984221708, -1007}, {10218702384817765436, -980},
	{15227053142812498563, -954}, {11345038669416679861, -927},
	{16905424996341287883, -901}, {12595523146049147757, -874},
	{9384396036005875287, -847}, {13983839803942852151, -821},
	{10418772551374772303, -794}, {15525180923007089351, -768},
	{11567161174868858868, -741}, {17236413322193710309, -715},
	{12842128665889583758, -688}, {9568131466127621947, -661},
	{14257626930069360058, -635}, {10622759856335341974, -608},
	{15829145694278690180, -582}, {11793632577567316726, -555},
	{17573882009934360870, -529}, {13093562431584567480, -502},
	{9755464219737475723, -475}, {14536774485912137811, -449},
	{10830740992659433045, -422}, {16139061738043178685, -396},
	{12024538023802026127, -369}, {17917957937422433684, -343},
	{13349918974505688015, -316}, {9946464728195732843, -289},
	{14821387422376473014, -263}, {11042794154864902060, -236},
	{16455045573212060422, -210}, {12259964326927110867, -183},
	{18268770466636286478, -157}, {13611294676837538539, -130},
	{10141204801825835212, -103}, {15111572745182864684, -77},
	{11258999068426240000, -50}, {16777216000000000000, -24},
	{12500000000000000000, 3}, {9313225746154785156, 30},
	{13877787807814456755, 56}, {10339757656912845936, 83},
	{15407439555097886824, 109}, {11479437019748901445, 136},
	{17105694144590052135, 162}, {12744735289059618216, 189},
	{9495567745759798747, 216}, {14149498560666738074, 242},
	{10542197943230523224, 269}, {15709099088952724970, 295},
	{11704190886730495818, 322}, {17440603504673385349, 348},
	{12994262207056124023, 375}, {9681479787123295682, 402},
	{14426529090290212157, 428}, {10748601772107342003, 455},
	{16016664761464807395, 481}, {11933345169920330789, 508},
	{17782069995880619868, 534}, {13248674568444952270, 561},
	{9871031767461413346, 588}, {14708983551653345445, 614},
	{10959046745042015199, 641}, {16330252207878254650, 667},
	{12166986024289022870, 694}, {18130221999122236476, 720},
	{13508068024458167312, 747}, {10064294952495520794, 774},
	{14996968138956309548, 800}, {11173611982879273257, 827},
	{16649979327439178909, 853}, {12405201291620119593, 880},
	{9242595204427927429, 907}, {13772540099066387757, 933},
	{10261342003245940623, 960}, {15290591125556738113, 986},
	{11392378155556871081, 1013}, {16975966327722178521, 1039},
	{12648080533535911531, 1066},
}
