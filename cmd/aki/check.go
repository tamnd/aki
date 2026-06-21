package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/rdb"
	"github.com/tamnd/aki/vfs"
)

// cmdCheck validates a file without importing it. With a plain path or --file it
// inspects an .aki file's header and meta snapshot. With --rdb it parses an RDB
// file and verifies its magic, opcodes, and CRC.
func cmdCheck(args []string) error {
	if len(args) == 2 && args[0] == "--rdb" {
		return checkRDB(args[1])
	}
	if len(args) == 2 && args[0] == "--file" {
		args = args[1:]
	}
	if len(args) != 1 {
		return errors.New("usage: aki check <file> | aki check --rdb <file>")
	}
	name := args[0]
	p, err := pager.Open(vfs.NewOS(), name, pager.Options{})
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	defer func() { _ = p.Close() }()

	h := p.Header()
	m := p.Meta()
	fmt.Printf("file:            %s\n", name)
	fmt.Printf("magic:           %q\n", string(h.Magic[:]))
	fmt.Printf("format_version:  %d\n", h.FormatVersion)
	fmt.Printf("page_size:       %d\n", h.PageSize)
	fmt.Printf("page_count:      %d\n", h.PageCount)
	fmt.Printf("db_count:        %d\n", h.DBCount)
	fmt.Printf("change_counter:  %d\n", h.ChangeCounter)
	fmt.Printf("freelist_head:   %s\n", pageRef(h.FreelistHead))
	fmt.Printf("freelist_count:  %d\n", h.FreelistCount)
	fmt.Printf("catalog_root:    %s\n", pageRef(h.CatalogRoot))
	fmt.Printf("default_codec:   %d\n", h.DefaultCodec)
	fmt.Printf("encryption_id:   %d\n", h.EncryptionID)
	fmt.Println("--- live meta ---")
	fmt.Printf("meta_seq:        %d\n", m.MetaSeq)
	fmt.Printf("txn_id:          %d\n", m.TxnID)
	fmt.Printf("wal_commit_lsn:  %d\n", m.WALCommitLSN)
	fmt.Printf("schema_version:  %d\n", m.SchemaVersion)
	for i, r := range m.DBRootPages {
		if r != format.NullPage {
			fmt.Printf("db[%d] root:      %s\n", i, pageRef(r))
		}
	}
	return nil
}

// checkRDB parses an RDB file and reports how many keys it holds across how many
// databases. A bad magic, version, opcode, or CRC comes back as an error so the
// process exits non-zero.
func checkRDB(name string) error {
	blob, err := os.ReadFile(name)
	if err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}
	snap, err := rdb.UnmarshalFile(blob)
	if err != nil {
		return fmt.Errorf("invalid RDB %s: %w", name, err)
	}
	keys := 0
	for _, db := range snap.DBs {
		keys += len(db.Entries)
	}
	fmt.Printf("file:       %s\n", name)
	fmt.Printf("format:     RDB\n")
	fmt.Printf("databases:  %d\n", len(snap.DBs))
	fmt.Printf("keys:       %d\n", keys)
	fmt.Printf("status:     OK\n")
	return nil
}

func pageRef(p uint32) string {
	if p == format.NullPage {
		return "(none)"
	}
	return fmt.Sprintf("%d", p)
}
