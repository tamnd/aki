# Lab: spin-before-park window

Spec 2064/f3/03 section 9, M0 lab 3.

## The question

An idle owner worker can spin on its inbound queue (next command caught in nanoseconds, core burned while idle) or park (core free, next command pays a wake round trip). Doc 03 pre-registers a 4us spin window with adaptive backoff (PRED-X7). Before the shard runtime bakes the window in: what does each window size actually buy in wake latency, and what does it cost in idle CPU, at low and high load?

## Method

`go run .` runs one pinned consumer polling a stamp ring with the doc 03 section 9.1 three-state protocol (running, spinning to a deadline, parked after a store-parked-then-recheck that closes the lost-wake race) and one pinned producer offering three loads: arrivals every 5us, arrivals every 50us, and back-to-back saturation. The window sweeps 0 (park immediately), 1us, 4us, 16us, 64us, and spin-forever. Each cell reports wake-to-receive p50/p99, parks per thousand messages, and spin burn, the share of wall time spent spinning empty, which is the idle CPU the window costs.

The park here is a Go channel receive because macOS has no raw futex; its wake cost measured ~10-16us p50 with a noisy scheduler tail, versus the 1-2us futex path doc 03 expects on Linux. That difference moves the crossover, which is the main reason the verdict below is provisional.

## Results

Apple M4 (4P + 6E), macOS, Go 1.26. Second of two runs shown; p50, parks, and burn were stable across runs, the parked-path p99 tail was not (hundreds of microseconds to milliseconds, macOS scheduler noise).

Low load, 5us between arrivals (200k msgs):

| window | p50 wake | p99 wake | parks/1k msgs | spin burn |
|---|---|---|---|---|
| 0s | 10µs | 642µs | 239 | 0% |
| 1µs | 10µs | 1.238ms | 231 | 5% |
| 4µs | 7µs | 396µs | 228 | 30% |
| 16µs | 0s | 184µs | 0 | 96% |
| 64µs | 0s | 828µs | 0 | 96% |
| forever | 0s | 1µs | 0 | 98% |

Idle-ish load, 50us between arrivals (40k msgs):

| window | p50 wake | p99 wake | parks/1k msgs | spin burn |
|---|---|---|---|---|
| 0s | 16µs | 740µs | 945 | 0% |
| 1µs | 16µs | 778µs | 941 | 2% |
| 4µs | 16µs | 1.036ms | 929 | 8% |
| 16µs | 16µs | 592µs | 934 | 30% |
| 64µs | 0s | 248µs | 2 | 98% |
| forever | 0s | 2µs | 0 | 100% |

Saturation, back-to-back (4M msgs):

| window | Mmsgs/s | parks/1k msgs |
|---|---|---|
| 0s | 15.8 | 0.00 |
| 4µs | 15.5 | 0.00 |

The shape: a window pays only when it reaches past the arrival gap. Below the gap it burns CPU proportional to window/gap and still parks on nearly every quiet period, so latency does not move; at or above the gap parks vanish and wake latency drops to the spin-catch floor, at near-total idle burn. At saturation the queue never empties, parks are zero at any window, and throughput is unchanged, confirming the window is purely an idle-regime knob.

## Provisional verdict

Laptop numbers, hypothesis until the GamingPC rerun. A fixed window is the wrong shape and the sweep shows why: every value either wastes its burn (window below the traffic's gap) or spins a core flat out (window above it). The doc 03 design, a 4us window under adaptive backoff so recent-park history decides whether to spin at all, is the only policy these curves support, and the lab keeps 4us as a defensible starting size: it is the largest window whose worst-case burn stayed under a third of a core here. The absolute crossover cannot be settled on this box because the channel park costs ~10-16us p50 where the Linux futex path should cost 1-2us, which shrinks what spinning is worth; the gate box rerun with the real futex decides the final window, and PRED-X7's three targets (near-zero wakes at P16, under 1us p50 delta at P1/50 conns, under 1 percent idle CPU) are all measurable there with this harness shape.

## Gate box results

GamingPC: i9-13900K (Raptor Lake, 8P + 16E), WSL2 Debian on Windows 11 (kernel 6.18.33.2-microsoft-standard-WSL2), Go 1.26.0, aki fc4a79f, `taskset -c 0-15` (P-core threads). One honesty note first: the harness parks on a Go channel on every OS, so what Linux buys here is the runtime's futex-backed channel wake, not a raw 1-2us futex round trip. Under WSL2 that path measured ~45us p50 wake at low load (a narrower 2-CPU mask gave ~34-38us), which is worse than the laptop's 10-16us p50, though the p99 tail is far tighter here (~100-140us against the laptop's milliseconds).

Low load, 5us between arrivals (200k msgs):

| window | p50 wake | p99 wake | parks/1k msgs | spin burn |
|---|---|---|---|---|
| 0s | 48µs | 144µs | 51 | 0% |
| 1µs | 45µs | 100µs | 51 | 1% |
| 4µs | 46µs | 134µs | 50 | 5% |
| 16µs | 151ns | 116µs | 1 | 94% |
| 64µs | 141ns | 110µs | 0 | 97% |
| forever | 135ns | 42µs | 0 | 99% |

Idle-ish load, 50us between arrivals (40k msgs):

| window | p50 wake | p99 wake | parks/1k msgs | spin burn |
|---|---|---|---|---|
| 0s | 77µs | 112µs | 489 | 0% |
| 1µs | 76µs | 102µs | 487 | 1% |
| 4µs | 66µs | 105µs | 485 | 4% |
| 16µs | 40µs | 125µs | 330 | 17% |
| 64µs | 152ns | 92µs | 3 | 98% |
| forever | 142ns | 226ns | 0 | 100% |

Saturation, back-to-back (4M msgs): 8-9 Mmsgs/s at both window 0 and 4us, zero parks, same as the laptop shape (the absolute rate is lower because the ring bounces between two physical cores here instead of an M4 cluster).

The curve shape is identical to the laptop: a window pays nothing until it reaches the arrival gap, then parks vanish and wake drops to the ~150ns spin catch. The crossover arithmetic on this box: a park costs ~45us p50 on the channel path, so spinning is worth it whenever the expected gap is under tens of microseconds, which is even more spin-friendly than the laptop, not less.

## Gate box verdict

The 4us starting window under adaptive backoff stands. The raw futex crossover this rerun owed is still not settled, because the harness has no raw-futex park to measure; what the box settles is that the real Linux park the runtime gives an idle owner costs ~45us p50 under WSL2, so the doc 03 assumption that parking is cheap (1-2us) is wrong on this box and the adaptive spin policy earns more than expected. If the shard runtime ever wants the crossover against a raw futex, the harness needs a futex(2) park mode; until then PRED-X7's targets stay measurable with this shape.
