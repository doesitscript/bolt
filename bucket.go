package bolt

import (
	"fmt"
	"strings"

	"github.com/Sirupsen/logrus"
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 32768

	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = (1 << 31) - 2
)

// Bucket represents a collection of key/value pairs inside the database.
type Bucket struct {
	name    string
	tx      *Tx                // the associated transaction
	buckets map[string]*Bucket // subbucket cache

	sequence uint64 // monotonically incrementing, used by NextSequence()

	// Sets the threshold for filling nodes when they split. By default,
	// the bucket will fill to 50% but it can be useful to increase this
	// amount if you know that your write workloads are mostly append-only.
	//
	// This is non-persisted across transactions so it must be set in every Tx.
	FillPercent float64
}

// Tx returns the tx of the bucket.
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Root returns the root of the bucket.
func (b *Bucket) Root() string {
	return b.name
}

// Writable returns whether the bucket is writable.
func (b *Bucket) Writable() bool {
	return !b.tx.db.readOnly
}

// Cursor creates a cursor associated with the bucket.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
func (b *Bucket) Cursor() *Cursor {
	// Allocate and return a cursor.
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// Bucket retrieves a nested bucket by name.
// Returns nil if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) Bucket(name []byte) *Bucket {
	if b.buckets != nil {
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}

	// Otherwise create a bucket and cache it.
	child := Bucket{tx: b.tx, name: string(name)}
	if b.buckets == nil {
		b.buckets = make(map[string]*Bucket)
	}
	b.buckets[string(name)] = &child

	return &child
}

// CreateBucket creates a new bucket at the given key and returns the new bucket.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx == nil {
		return nil, ErrTxClosed
	} else if !b.Writable() {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	// Check if the table already exists.
	rows, err := b.tx.db.file.Query(fmt.Sprintf(`SELECT name FROM sqlite_master WHERE type='table' AND name=%s`, string(key)))
	if err == nil {
		defer rows.Close()

		if rows.Next() {
			// The table exists, return the correct error.
			return nil, ErrBucketExists
		}
	}

	// Create the table.
	_, err = b.tx.db.file.Exec(fmt.Sprintf(`CREATE table IF NOT EXISTS %s (id integer NOT NULL PRIMARY KEY, key text NOT NULL unique, value text);`, string(key)))
	if err != nil {
		return nil, err
	}

	return b.Bucket(key), nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns a reference to it.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	if err == ErrBucketExists {
		return b.Bucket(key), nil
	} else if err != nil {
		return nil, err
	}
	return child, nil
}

// DeleteBucket deletes a bucket at the given key.
// Returns an error if the bucket does not exists, or if the key represents a non-bucket value.
func (b *Bucket) DeleteBucket(key []byte) error {
	if b.tx == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Drop the table.
	_, err := b.tx.db.file.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, string(key)))
	if err != nil {
		return err
	}

	// Remove cached copy.
	delete(b.buckets, string(key))

	return nil
}

// Get retrieves the value for a key in the bucket.
// Returns a nil value if the key does not exist or if the key is a nested bucket.
// The returned value is only valid for the life of the transaction.
func (b *Bucket) Get(key []byte) []byte {
	rows, err := b.tx.db.file.Query(fmt.Sprintf("SELECT id, key, value FROM %s WHERE key=%q", b.name, string(key)))
	if err != nil {
		logrus.Errorf("querying value for key %s failed: %v", string(key), err)
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id    int
			key   string
			value string
		)
		if err = rows.Scan(&id, &key, &value); err != nil {
			logrus.Errorf("scanning rows failed: %v", err)
			return nil
		}

		return []byte(value)
	}
	if err = rows.Err(); err != nil {
		logrus.Errorf("rows failed: %v", err)
		return nil
	}

	return nil
}

// Put sets the value for a key in the bucket.
// If the key exist then its previous value will be overwritten.
// Supplied value must remain valid for the life of the transaction.
// Returns an error if the bucket was created from a read-only transaction, if the key is blank, if the key is too large, or if the value is too large.
func (b *Bucket) Put(key []byte, value []byte) error {
	if b.tx == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	_, err := b.tx.db.file.Exec(fmt.Sprintf(`INSERT into %s (key, value) values(%q, %q);`, b.name, string(key), string(value)))
	if err != nil {
		// If it failed because the key already exists, then update it.
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			_, err = b.tx.db.file.Exec(fmt.Sprintf(`UPDATE %s SET value = %q WHERE key = %q;`, b.name, string(value), string(key)))
		}

		return err
	}

	return nil
}

// Delete removes a key from the bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket was created from a read-only transaction.
func (b *Bucket) Delete(key []byte) error {
	if b.tx == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	_, err := b.tx.db.file.Exec(fmt.Sprintf(`DELETE FROM %s WHERE key = %q;`, b.name, string(key)))
	if err != nil {
		return err
	}

	return nil
}

// Sequence returns the current integer for the bucket without incrementing it.
func (b *Bucket) Sequence() uint64 { return b.sequence }

// SetSequence updates the sequence number for the bucket.
func (b *Bucket) SetSequence(v uint64) error {
	if b.tx == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	b.sequence = v
	return nil
}

// NextSequence returns an autoincrementing integer for the bucket.
func (b *Bucket) NextSequence() (uint64, error) {
	if b.tx == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	// Increment and return the sequence.
	b.sequence++
	return b.sequence, nil
}

// ForEach executes a function for each key/value pair in a bucket.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller. The provided function must not modify
// the bucket; this will result in undefined behavior.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx == nil {
		return ErrTxClosed
	}

	rows, err := b.tx.db.file.Query(fmt.Sprintf("SELECT id, key, value FROM %s", b.name))
	if err != nil {
		return fmt.Errorf("querying for all key values failed: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id    int
			key   string
			value string
		)
		if err = rows.Scan(&id, &key, &value); err != nil {
			return fmt.Errorf("scanning rows failed: %v", err)
		}

		if err := fn([]byte(key), []byte(value)); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("rows failed: %v", err)
	}

	return nil
}

// Stat returns stats on a bucket.
func (b *Bucket) Stats() BucketStats {
	return BucketStats{}
}

// BucketStats records statistics about resources used by a bucket.
type BucketStats struct {
	// Page count statistics.
	BranchPageN     int // number of logical branch pages
	BranchOverflowN int // number of physical branch overflow pages
	LeafPageN       int // number of logical leaf pages
	LeafOverflowN   int // number of physical leaf overflow pages

	// Tree statistics.
	KeyN  int // number of keys/value pairs
	Depth int // number of levels in B+tree

	// Page size utilization.
	BranchAlloc int // bytes allocated for physical branch pages
	BranchInuse int // bytes actually used for branch data
	LeafAlloc   int // bytes allocated for physical leaf pages
	LeafInuse   int // bytes actually used for leaf data

	// Bucket statistics
	BucketN           int // total number of buckets including the top bucket
	InlineBucketN     int // total number on inlined buckets
	InlineBucketInuse int // bytes used for inlined buckets (also accounted for in LeafInuse)
}

func (s *BucketStats) Add(other BucketStats) {
}
