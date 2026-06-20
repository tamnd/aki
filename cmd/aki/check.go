package main

import (
	"errors"
	"fmt"

	"github.com/tamnd/aki/format"
	"github.com/tamnd/aki/pager"
	"github.com/tamnd/aki/vfs"
)

// cmdCheck opens an .aki file read-only and prints its header and live meta
// snapshot, the first line of defense for diagnosing a file on disk.
func cmdCheck(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: aki check <file>")
	}
	name := args[0]
	p, err := pager.Open(vfs.NewOS(), name, pager.Options{})
	if err != nil {
		return fmt.Errorf("open %s: %w", name, err)
	}
	defer p.Close()

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

func pageRef(p uint32) string {
	if p == format.NullPage {
		return "(none)"
	}
	return fmt.Sprintf("%d", p)
}
