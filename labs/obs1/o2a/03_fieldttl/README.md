# fieldttl: the inline field-TTL column and the pay-only-if-used check

## Question

Doc 08 section 3 puts hash field TTLs (the HEXPIRE family) inline in the field's chunk so any reader can apply the lazy expiry rule with no side lookups, and doc 08 section 8 keeps them out of the segment TTL class.
The hashes slice has to pick the chunk encoding that carries them.
This lab prices three candidates against the pay-only-if-used bar: a hash that never uses HEXPIRE should pay nothing, and a hash that uses it sparsely should pay close to the 8 B per bearer floor.
The burden arm prices the lazy rule itself: expired fields ride in the object until a rewrite, and both live and expired probes must still cost one GET.

## Method

Real store chunk frames on the counting sim, the same disclosed lab models as the other O2a labs: lab-local element packing (disc u64, flen u16, vlen u16, field, value), million-field hash, 15 B fields, 64 B values, 16 KiB chunk target, 128 KiB blocks.
Encodings: column adds an 8 B expiry slot to every element of any chunk holding at least one bearer (chunk flag); flag adds 1 B to every element and 8 B more on bearers; bitmap puts a ceil(count/8) B presence map at the head of contaminated chunks and 8 B on bearers only.
TTL assignment and expiry are deterministic per-field draws, so runs reproduce.
Overhead is measured as whole-object bytes over the plain packing, so chunk-count growth and block padding are in the bill, not just payload arithmetic.

## Prediction (PRED-OBS1-O2A-FIELDTTL, filed before the scored run)

At one million fields, TTL-use fraction f in permille {0, 1, 10, 100, 500, 1000}:

1. Pay-only-if-used: column and bitmap at f=0 are byte-identical to the plain packing, 0.000 B per element; flag pays 1.05 to 1.15 B at f=0 and never below 1 B anywhere.
2. Contamination poisons the column: with ~180 elements per chunk, one bearer per thousand fields contaminates 20 to 24% of chunks and costs 1.5 to 1.9 B per element; one per hundred contaminates 84 to 89% for 7.5 to 8.1 B; from 10% use it is 100% contamination at 8.9 to 9.4 B, the all-in cost regardless of use.
3. Bitmap tracks the floor: 0.00 to 0.04 B at f=1 permille, 0.03 to 0.10 at 10, 1.0 to 1.15 at 100, 4.4 to 4.7 at 500, 9.0 to 9.3 at full use, meeting the column only when every field bears a TTL.
4. Flag sits between: 1 + 8f plus packing growth, 1.3 to 1.45 B at 10 permille, 2.1 to 2.35 at 100, 5.5 to 5.9 at 500, 10.2 to 10.7 at full use, always the worst at low use.
5. The point bill never moves: 1.0000 GETs per op and 100% found at every f under every encoding.
6. Burden at expired fraction e in permille {250, 500, 900} on an all-TTL bitmap corpus: dead share by count within 0.5 points of e/10; live probes 1.0000 GETs at 100% found and expired probes 1.0000 GETs at 0% found (the lazy rule's price is the fetch, not extra fetches); the rewrite reclaims within 0.5 points of the dead share; scan requests sit on the ceil(bytes/16MiB) identity both before and after the rewrite.

Kill line: an encoding charging a TTL-free hash more than rounding breaks pay-only-if-used and is out; any probe off 1.0000 GETs kills the inline claim; an expired probe costing more than one GET before rewrite kills the lazy rule.

## Calibration disclosure

A -quick run at 10^5 fields executed during development after the bands were derived from the packing arithmetic; the bands above fold in its whole-object measurement (padding and chunk-count growth push full-use cost to ~9.2 B, not the naive 8), and the burden and identity rows matched.

## Run

    ./run.sh

## Results

Scored on the M4 box, fieldttl.csv checked in.

Pay-only-if-used: column and bitmap at zero TTL use are byte-identical to the plain packing (0.000 B per element, the smoke test asserts the exact bytes); flag pays 1.058 B forever.
The column poisons at sparse use: one bearer per thousand fields contaminates 17.9% of chunks for 1.453 B per element, one per hundred contaminates 86.7% for 7.730 B, and from 10% use it is the full 9.164 B no matter how few fields need it.
Band miss, disclosed: the one-per-thousand cells landed just under their 20-24% and 1.5-1.9 B bands, which were anchored to the noisy 10^5 calibration (21.7% over ~590 chunks); the converged million-field value matches the 1-(1-f)^m contamination arithmetic directly, so the miss is the calibration sample, not the model, and the poisoning claim stands slightly softened.
The bitmap tracks the floor: 0.000 at one permille, 0.068 at 1%, 1.030 at 10%, 4.620 at half, 9.164 at full use, meeting the column only when every field bears a TTL.
The flag encoding is worst at low use and worst at full use (10.435 B).
The point bill never moved: 1.0000 GETs per op, 100% found, at every fraction under every encoding.
Burden: dead share by count exactly 25.0/50.0/90.0 at the three expiry fractions, live probes 1.0000 GETs at 100% found, expired probes 1.0000 GETs at 0% found, the rewrite reclaimed exactly the dead share (103.2 MiB down to 77.4/51.6/10.3), and scan requests sat on the ceil identity on both sides of the rewrite.

## Verdict

PRED-OBS1-O2A-FIELDTTL: HIT, with the column one-permille cells' low miss disclosed.
The hashes slice bakes the bitmap-per-contaminated-chunk encoding: zero cost when HEXPIRE is unused, near-floor cost when sparse, and the lazy rule's whole price is the one GET the probe already pays, with rewrite reclaiming dead fields byte for byte.
