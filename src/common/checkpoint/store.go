package checkpoint

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	fileExt = ".ckpt"     // committed overwrite checkpoint
	tmpExt  = ".ckpt.tmp" // staged (not-yet-committed) overwrite checkpoint
	logExt  = ".log"      // append-only record log

	overwriteVersion   = 1
	overwriteHeaderLen = 1 + 8 // version(1) | gen(8)

	// record framing: len(4) | gen(8) | crc32c(4) | payload(len)
	recordHeaderLen = 4 + 8 + 4
)

// crcTable is Castagnoli (crc32c) — cheap, hardware-accelerated, and used to
// detect a torn trailing record in an append-only log after a crash.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// baseStore holds the bits every store shares: the directory it owns, a mutex
// serialising its writers, and the directory/listing/path helpers.
type baseStore struct {
	dir string
	mu  sync.Mutex
}

func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating checkpoint dir: %w", err)
	}
	return nil
}

func (s *baseStore) path(key, ext string) string {
	return filepath.Join(s.dir, key+ext)
}

// syncDir fsyncs the directory so a create/rename/delete of a file within it is
// itself durable — fsyncing the file alone does not persist its directory entry.
func (s *baseStore) syncDir() error {
	d, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("opening dir for sync: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("syncing dir: %w", err)
	}
	return nil
}

func (s *baseStore) listKeys(ext string) ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("listing checkpoints: %w", err)
	}
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ext) {
			continue
		}
		keys = append(keys, strings.TrimSuffix(name, ext))
	}
	return keys, nil
}

func validKey(key string) error {
	if key == "" || key == "." || key == ".." || strings.ContainsAny(key, `/\`) {
		return fmt.Errorf("invalid checkpoint key: %q", key)
	}
	return nil
}

// writeFileSync writes data to path (truncating any existing file), fsyncs the
// file, and closes it. It does not fsync the directory; callers that need the
// new directory entry to be durable must call syncDir themselves.
func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("opening %s: %w", filepath.Base(path), err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("writing %s: %w", filepath.Base(path), err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return fmt.Errorf("syncing %s: %w", filepath.Base(path), err)
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return fmt.Errorf("closing %s: %w", filepath.Base(path), err)
	}
	return nil
}

func fsyncPath(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("opening %s for sync: %w", filepath.Base(path), err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing %s: %w", filepath.Base(path), err)
	}
	return nil
}

func removeIfExists(path string) (bool, error) {
	err := os.Remove(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// FileStore is the legacy single-blob overwrite store. It is retained only so the
// existing Manager keeps working until the coordinator is rewired onto
// OverwriteStore/AppendStore, after which FileStore and Manager are both removed.
type FileStore struct {
	baseStore
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := ensureDir(dir); err != nil {
		return nil, err
	}
	return &FileStore{baseStore: baseStore{dir: dir}}, nil
}

func (s *FileStore) Save(key string, data []byte) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tmp := s.path(key, tmpExt)
	if err := writeFileSync(tmp, data); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path(key, fileExt)); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("renaming checkpoint: %w", err)
	}
	return s.syncDir()
}

func (s *FileStore) Load(key string) ([]byte, bool, error) {
	if err := validKey(key); err != nil {
		return nil, false, err
	}
	data, err := os.ReadFile(s.path(key, fileExt))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("reading checkpoint: %w", err)
	}
	return data, true, nil
}

func (s *FileStore) Delete(key string) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	removed, err := removeIfExists(s.path(key, fileExt))
	if err != nil {
		return fmt.Errorf("deleting checkpoint: %w", err)
	}
	if removed {
		return s.syncDir()
	}
	return nil
}

func (s *FileStore) Keys() ([]string, error) {
	return s.listKeys(fileExt)
}

// OverwriteStore is a double-buffered, generation-stamped overwrite store for
// state that is fully rewritten each checkpoint (max/avg/count aggregates, eof
// counters). A checkpoint is written in two phases so it can be reconciled
// against the commit marker after a crash:
//
//	Stage   — write key.ckpt.tmp {version|gen|data}; the live key.ckpt is untouched.
//	Promote — once the generation is committed, rename key.ckpt.tmp -> key.ckpt.
//
// The previous committed file survives the whole Stage window, so a flush that
// crashes before it commits rolls back to it; one that commits but crashes before
// Promote is finished idempotently on recovery.
type OverwriteStore struct {
	baseStore
}

func NewOverwriteStore(dir string) (*OverwriteStore, error) {
	if err := ensureDir(dir); err != nil {
		return nil, err
	}
	return &OverwriteStore{baseStore: baseStore{dir: dir}}, nil
}

// Stage writes the staged checkpoint for the next generation. The staged file and
// its directory entry are made durable here, because the commit (the seen frame)
// is appended afterwards: were the stage lost, the commit could point at state
// that no longer exists, silently dropping that generation.
func (s *OverwriteStore) Stage(key string, gen uint64, data []byte) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := writeFileSync(s.path(key, tmpExt), encodeOverwrite(gen, data)); err != nil {
		return err
	}
	return s.syncDir()
}

// Promote reconciles the staged file against the committed generation. If the
// stage carries the committed generation it is materialised (rename -> key.ckpt);
// otherwise — no stage, a torn stage, or a generation that never committed — any
// staged file is dropped so it can never be mistaken for committed state. The
// coordinator calls this both in a flush's apply phase and during recovery.
func (s *OverwriteStore) Promote(key string, committedGen uint64) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tmp := s.path(key, tmpExt)
	gen, valid, err := readOverwriteHeader(tmp)
	if err != nil {
		return err
	}
	if valid && gen == committedGen {
		if err := os.Rename(tmp, s.path(key, fileExt)); err != nil {
			return fmt.Errorf("promoting checkpoint: %w", err)
		}
		return s.syncDir()
	}
	removed, err := removeIfExists(tmp)
	if err != nil {
		return fmt.Errorf("discarding stale staged checkpoint: %w", err)
	}
	if removed {
		return s.syncDir()
	}
	return nil
}

// Load returns the committed checkpoint and the generation it was written at.
func (s *OverwriteStore) Load(key string) ([]byte, uint64, bool, error) {
	if err := validKey(key); err != nil {
		return nil, 0, false, err
	}
	raw, err := os.ReadFile(s.path(key, fileExt))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, fmt.Errorf("reading checkpoint: %w", err)
	}
	data, gen, err := decodeOverwrite(raw)
	if err != nil {
		return nil, 0, false, err
	}
	return data, gen, true, nil
}

// Delete removes both the committed and any staged checkpoint for the key.
func (s *OverwriteStore) Delete(key string) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	removedCkpt, err := removeIfExists(s.path(key, fileExt))
	if err != nil {
		return fmt.Errorf("deleting checkpoint: %w", err)
	}
	removedTmp, err := removeIfExists(s.path(key, tmpExt))
	if err != nil {
		return fmt.Errorf("deleting staged checkpoint: %w", err)
	}
	if removedCkpt || removedTmp {
		return s.syncDir()
	}
	return nil
}

func (s *OverwriteStore) Keys() ([]string, error) {
	return s.listKeys(fileExt)
}

func encodeOverwrite(gen uint64, data []byte) []byte {
	buf := make([]byte, overwriteHeaderLen+len(data))
	buf[0] = overwriteVersion
	binary.BigEndian.PutUint64(buf[1:overwriteHeaderLen], gen)
	copy(buf[overwriteHeaderLen:], data)
	return buf
}

func decodeOverwrite(raw []byte) ([]byte, uint64, error) {
	if len(raw) < overwriteHeaderLen {
		return nil, 0, fmt.Errorf("overwrite checkpoint too short: %d bytes", len(raw))
	}
	if raw[0] != overwriteVersion {
		return nil, 0, fmt.Errorf("unsupported overwrite checkpoint version %d", raw[0])
	}
	return raw[overwriteHeaderLen:], binary.BigEndian.Uint64(raw[1:overwriteHeaderLen]), nil
}

// readOverwriteHeader reads only the {version|gen} header of a staged file, so
// Promote can decide without loading a potentially large state body. valid is
// false when the file is absent or its header is torn/unrecognised.
func readOverwriteHeader(path string) (gen uint64, valid bool, err error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("opening staged checkpoint: %w", err)
	}
	defer f.Close()

	var hdr [overwriteHeaderLen]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, false, nil // torn/incomplete stage
		}
		return 0, false, fmt.Errorf("reading staged header: %w", err)
	}
	if hdr[0] != overwriteVersion {
		return 0, false, nil
	}
	return binary.BigEndian.Uint64(hdr[1:overwriteHeaderLen]), true, nil
}

// Record is one framed entry in an append-only log: a generation-stamped payload.
type Record struct {
	Gen     uint64
	Payload []byte
}

// AppendStore is a generation-stamped, append-only record log for state that
// grows monotonically (dedup seen sets, scatter/gather accumulations). Each flush
// appends one framed record; recovery keeps records up to the committed
// generation and truncates the staged-but-uncommitted tail.
type AppendStore struct {
	baseStore
}

func NewAppendStore(dir string) (*AppendStore, error) {
	if err := ensureDir(dir); err != nil {
		return nil, err
	}
	return &AppendStore{baseStore: baseStore{dir: dir}}, nil
}

// Append writes one framed record and fsyncs the log. A brand-new log also fsyncs
// the directory so its entry is durable; appends to an existing log are covered by
// the file fsync. The frame's length and crc make a torn tail detectable, which
// is what makes the write crash-safe (O_APPEND alone is not).
func (s *AppendStore) Append(key string, gen uint64, payload []byte) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	logPath := s.path(key, logExt)
	_, statErr := os.Stat(logPath)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return fmt.Errorf("stat log: %w", statErr)
	}

	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("opening log: %w", err)
	}
	if _, err := f.Write(encodeRecord(gen, payload)); err != nil {
		f.Close()
		return fmt.Errorf("appending record: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("syncing log: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing log: %w", err)
	}
	if created {
		return s.syncDir()
	}
	return nil
}

// Load returns every intact record in order and the highest generation seen. It
// stops at the first torn/corrupt frame (the end of the durable prefix) and does
// not modify the file. For the seen log, the returned maxGen is the committed
// generation watermark.
func (s *AppendStore) Load(key string) ([]Record, uint64, error) {
	if err := validKey(key); err != nil {
		return nil, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	raw, err := os.ReadFile(s.path(key, logExt))
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("reading log: %w", err)
	}

	var recs []Record
	var maxGen uint64
	for off := 0; ; {
		gen, payload, next, ok := decodeRecordAt(raw, off)
		if !ok {
			break
		}
		p := make([]byte, len(payload))
		copy(p, payload)
		recs = append(recs, Record{Gen: gen, Payload: p})
		if gen > maxGen {
			maxGen = gen
		}
		off = next
	}
	return recs, maxGen, nil
}

// Truncate drops every record with gen > committedGen along with any torn trailing
// bytes, leaving the log at exactly the committed prefix. It is idempotent: a
// crash mid-truncate re-derives the same cut on the next run.
func (s *AppendStore) Truncate(key string, committedGen uint64) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	logPath := s.path(key, logExt)
	raw, err := os.ReadFile(logPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading log: %w", err)
	}

	validEnd := 0
	for off := 0; ; {
		gen, _, next, ok := decodeRecordAt(raw, off)
		if !ok || gen > committedGen {
			break
		}
		validEnd = next
		off = next
	}
	if validEnd == len(raw) {
		return nil
	}
	if err := os.Truncate(logPath, int64(validEnd)); err != nil {
		return fmt.Errorf("truncating log: %w", err)
	}
	return fsyncPath(logPath)
}

func (s *AppendStore) Delete(key string) error {
	if err := validKey(key); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	removed, err := removeIfExists(s.path(key, logExt))
	if err != nil {
		return fmt.Errorf("deleting log: %w", err)
	}
	if removed {
		return s.syncDir()
	}
	return nil
}

func (s *AppendStore) Keys() ([]string, error) {
	return s.listKeys(logExt)
}

func encodeRecord(gen uint64, payload []byte) []byte {
	rec := make([]byte, recordHeaderLen+len(payload))
	binary.BigEndian.PutUint32(rec[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint64(rec[4:12], gen)
	copy(rec[recordHeaderLen:], payload)
	crc := crc32.Update(0, crcTable, rec[4:12])
	crc = crc32.Update(crc, crcTable, payload)
	binary.BigEndian.PutUint32(rec[12:16], crc)
	return rec
}

// decodeRecordAt parses the framed record at off. ok is false when there is no
// intact record there — a torn length/payload or a crc mismatch — which for an
// append-only log marks the end of the durable prefix.
func decodeRecordAt(raw []byte, off int) (gen uint64, payload []byte, next int, ok bool) {
	if off+recordHeaderLen > len(raw) {
		return 0, nil, off, false
	}
	plen := int(binary.BigEndian.Uint32(raw[off : off+4]))
	end := off + recordHeaderLen + plen
	if plen < 0 || end > len(raw) {
		return 0, nil, off, false
	}
	gen = binary.BigEndian.Uint64(raw[off+4 : off+12])
	crc := binary.BigEndian.Uint32(raw[off+12 : off+16])
	payload = raw[off+recordHeaderLen : end]
	want := crc32.Update(0, crcTable, raw[off+4:off+12])
	want = crc32.Update(want, crcTable, payload)
	if want != crc {
		return 0, nil, off, false
	}
	return gen, payload, end, true
}
