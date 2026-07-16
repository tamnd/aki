package akifile

import (
	"io"
	"os"
	"time"
)

// Device is the minimal file surface the writer needs, factored out so a test
// can drive the append path against an in-memory buffer and the real store
// against an *os.File. Offsets are absolute; Sync forces durability to media.
type Device interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Truncate(size int64) error
	Size() (int64, error)
	Close() error
}

// osDevice adapts an *os.File to Device, adding only the Size accessor the
// interface needs (the rest is already on *os.File).
type osDevice struct{ *os.File }

func (d osDevice) Size() (int64, error) {
	fi, err := d.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// SyncPolicy controls when the writer forces a group of segments to stable media
// after it lays them down (spec 2064/f3/07 section 8, appendfsync).
type SyncPolicy uint8

const (
	// SyncAlways fsyncs after every appended group before the offsets return: an
	// acked write is durable. The strongest mode; the group-commit batching in
	// section 2 is what keeps its flush rate affordable.
	SyncAlways SyncPolicy = iota
	// SyncEverySec fsyncs at most once per interval (default one second), so a
	// crash loses at most the last interval's un-synced tail.
	SyncEverySec
	// SyncNo never fsyncs from the append path; durability rides the OS cache and
	// an explicit Sync or Close. Fastest, weakest.
	SyncNo
)

// CreateOptions carries what identifies a fresh file plus its durability policy.
// Everything else in the prefix is a fixed default (NewPrefix).
type CreateOptions struct {
	ShardCount       uint32
	SepThreshold     uint32
	UUID             [16]byte
	CreatedUnixNanos uint64
	Sync             SyncPolicy
	SyncInterval     time.Duration // SyncEverySec window; 0 means one second
	Now              func() time.Time
}

// OpenOptions carries the durability policy an existing file is reopened under;
// the format constants come from the file's own prefix.
type OpenOptions struct {
	Sync         SyncPolicy
	SyncInterval time.Duration
	Now          func() time.Time
}

// File is an open .aki file positioned for append: it owns the append cursor and
// the writer-assigned global_seq counter. It is a single-writer object by
// design (one log-writer goroutine, spec 2064/f3/07 section 2), so it holds no
// lock and the caller must not append from two goroutines at once.
type File struct {
	dev       Device
	prefix    *Prefix
	cursor    uint64
	globalSeq uint64

	sync     SyncPolicy
	interval time.Duration
	now      func() time.Time
	lastSync time.Time
}

// Create makes a fresh .aki file: it writes the immutable prefix and both meta
// slots into the 16KiB header page, sizes the file to the header page, and
// fsyncs so an empty-but-valid file survives a crash right after create. It
// fails if the file already exists (O_EXCL); a half-written file is removed.
func Create(path string, opts CreateOptions) (*File, error) {
	fh, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	f, err := CreateOnDevice(osDevice{fh}, opts)
	if err != nil {
		_ = fh.Close()
		_ = os.Remove(path)
		return nil, err
	}
	return f, nil
}

// CreateOnDevice is the device-level core of Create, exported so a test can
// build a file over an in-memory device and count its fsyncs.
func CreateOnDevice(dev Device, opts CreateOptions) (*File, error) {
	prefix := NewPrefix(opts.ShardCount, opts.SepThreshold, opts.UUID, opts.CreatedUnixNanos)
	if _, err := dev.WriteAt(prefix.Marshal(), 0); err != nil {
		return nil, err
	}
	// Both slots start identical and valid: commit_seq 0, an empty append space
	// whose durable size is the header page. The checkpoint slice maintains them.
	meta := &MetaSlot{FileSize: PageSize, CleanShutdown: 1}
	mb, err := meta.Marshal(prefix.ChecksumKind)
	if err != nil {
		return nil, err
	}
	if _, err := dev.WriteAt(mb, int64(prefix.MetaSlotAOff)); err != nil {
		return nil, err
	}
	if _, err := dev.WriteAt(mb, int64(prefix.MetaSlotBOff)); err != nil {
		return nil, err
	}
	if err := dev.Truncate(PageSize); err != nil {
		return nil, err
	}
	if err := dev.Sync(); err != nil {
		return nil, err
	}
	return newFile(dev, prefix, opts.Sync, opts.SyncInterval, opts.Now, PageSize, 0), nil
}

// Open reopens an existing .aki file for append. It validates the prefix, then
// finds the durable append tail by a forward scan of the append space (scanTail)
// so the writer resumes past the last intact segment. Full per-shard parallel
// recovery (slice 5) builds on this scan; here it only bootstraps the cursor.
func Open(path string, opts OpenOptions) (*File, error) {
	fh, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	f, err := OpenOnDevice(osDevice{fh}, opts)
	if err != nil {
		_ = fh.Close()
		return nil, err
	}
	return f, nil
}

// OpenOnDevice is the device-level core of Open.
func OpenOnDevice(dev Device, opts OpenOptions) (*File, error) {
	hb := make([]byte, PrefixSize)
	if _, err := dev.ReadAt(hb, 0); err != nil {
		return nil, err
	}
	prefix, err := ParsePrefix(hb)
	if err != nil {
		return nil, err
	}
	size, err := dev.Size()
	if err != nil {
		return nil, err
	}
	cursor, seq := scanTail(dev, prefix, uint64(size))
	return newFile(dev, prefix, opts.Sync, opts.SyncInterval, opts.Now, cursor, seq), nil
}

func newFile(dev Device, prefix *Prefix, sync SyncPolicy, interval time.Duration, now func() time.Time, cursor, seq uint64) *File {
	if now == nil {
		now = time.Now
	}
	if interval <= 0 {
		interval = time.Second
	}
	return &File{
		dev:       dev,
		prefix:    prefix,
		cursor:    cursor,
		globalSeq: seq,
		sync:      sync,
		interval:  interval,
		now:       now,
		lastSync:  now(),
	}
}

// Cursor is the offset the next segment will be written at: the append tail.
func (f *File) Cursor() uint64 { return f.cursor }

// GlobalSeq is the highest global_seq the writer has assigned so far.
func (f *File) GlobalSeq() uint64 { return f.globalSeq }

// Prefix is the file's immutable header.
func (f *File) Prefix() *Prefix { return f.prefix }

// Sync forces a flush regardless of policy and resets the everysec window. It is
// how SyncEverySec and SyncNo make an explicit durability barrier.
func (f *File) Sync() error { return f.doSync() }

// Close flushes any un-synced tail and closes the underlying device.
func (f *File) Close() error {
	if err := f.doSync(); err != nil {
		_ = f.dev.Close()
		return err
	}
	return f.dev.Close()
}
