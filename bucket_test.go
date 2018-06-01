package bolt_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/quick"

	"github.com/boltdb/bolt"
)

// Ensure that a bucket that gets a non-existent key returns nil.
func TestBucket_Get_NonExistent(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if v := b.Get([]byte("foo")); v != nil {
			t.Fatal("expected nil value")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can read a value that is not flushed yet.
func TestBucket_Get_FromNode(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if v := b.Get([]byte("foo")); !bytes.Equal(v, []byte("bar")) {
			t.Fatalf("unexpected value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket retrieved via Get() returns a nil.
func TestBucket_Get_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo")); err != nil {
			t.Fatal(err)
		}

		if tx.Bucket([]byte("widgets")).Get([]byte("foo")) != nil {
			t.Fatal("expected nil value")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a slice returned from a bucket has a capacity equal to its length.
// This also allows slices to be appended to since it will require a realloc by Go.
//
// https://github.com/boltdb/bolt/issues/544
func TestBucket_Get_Capacity(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	// Write key to a bucket.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("bucket"))
		if err != nil {
			return err
		}
		return b.Put([]byte("key"), []byte("val"))
	}); err != nil {
		t.Fatal(err)
	}

	// Retrieve value and attempt to append to it.
	if err := db.Update(func(tx *bolt.Tx) error {
		k, v := tx.Bucket([]byte("bucket")).Cursor().First()

		// Verify capacity.
		if len(k) != cap(k) {
			t.Fatalf("unexpected key slice capacity: %d", cap(k))
		} else if len(v) != cap(v) {
			t.Fatalf("unexpected value slice capacity: %d", cap(v))
		}

		// Ensure slice can be appended to without a segfault.
		k = append(k, []byte("123")...)
		v = append(v, []byte("123")...)

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can write a key/value.
func TestBucket_Put(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}

		v := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		if !bytes.Equal([]byte("bar"), v) {
			t.Fatalf("unexpected value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can rewrite a key in the same transaction.
func TestBucket_Put_Repeat(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("baz")); err != nil {
			t.Fatal(err)
		}

		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		if !bytes.Equal([]byte("baz"), value) {
			t.Fatalf("unexpected value: %v", value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can write a bunch of large values.
func TestBucket_Put_Large(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	count, factor := 100, 200
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < count; i++ {
			if err := b.Put([]byte(strings.Repeat("0", i*factor)), []byte(strings.Repeat("X", (count-i)*factor))); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 1; i < count; i++ {
			value := b.Get([]byte(strings.Repeat("0", i*factor)))
			if !bytes.Equal(value, []byte(strings.Repeat("X", (count-i)*factor))) {
				t.Fatalf("unexpected value: %v", value)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a database can perform multiple large appends safely.
func TestDB_Put_VeryLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	n, batchN := 400000, 200000
	ksize, vsize := 8, 500

	db := MustOpenDB()
	defer db.MustClose()

	for i := 0; i < n; i += batchN {
		if err := db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("widgets"))
			if err != nil {
				t.Fatal(err)
			}
			for j := 0; j < batchN; j++ {
				k, v := make([]byte, ksize), make([]byte, vsize)
				binary.BigEndian.PutUint32(k, uint32(i+j))
				if err := b.Put(k, v); err != nil {
					t.Fatal(err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// Ensure that a setting a value on a key with a bucket value returns an error.
func TestBucket_Put_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		b0, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Bucket([]byte("widgets")).CreateBucket([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		if err := b0.Put([]byte("foo"), []byte("bar")); err != bolt.ErrIncompatibleValue {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a setting a value while the transaction is closed returns an error.
func TestBucket_Put_Closed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	b, err := tx.CreateBucket([]byte("widgets"))
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := b.Put([]byte("foo"), []byte("bar")); err != bolt.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that setting a value on a read-only bucket returns an error.
func TestBucket_Put_ReadOnly(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		if err := b.Put([]byte("foo"), []byte("bar")); err != bolt.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can delete an existing key.
func TestBucket_Delete(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		if v := b.Get([]byte("foo")); v != nil {
			t.Fatalf("unexpected value: %v", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that deleting a large set of keys will work correctly.
func TestBucket_Delete_Large(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 100; i++ {
			if err := b.Put([]byte(strconv.Itoa(i)), []byte(strings.Repeat("*", 1024))); err != nil {
				t.Fatal(err)
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 100; i++ {
			if err := b.Delete([]byte(strconv.Itoa(i))); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("widgets"))
		for i := 0; i < 100; i++ {
			if v := b.Get([]byte(strconv.Itoa(i))); v != nil {
				t.Fatalf("unexpected value: %v, i=%d", v, i)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that deleting a bucket using Delete() returns an error.
func TestBucket_Delete_Bucket(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.CreateBucket([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete([]byte("foo")); err != bolt.ErrIncompatibleValue {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that deleting a key on a read-only bucket returns an error.
func TestBucket_Delete_ReadOnly(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		if err := tx.Bucket([]byte("widgets")).Delete([]byte("foo")); err != bolt.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a deleting value while the transaction is closed returns an error.
func TestBucket_Delete_Closed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	b, err := tx.CreateBucket([]byte("widgets"))
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := b.Delete([]byte("foo")); err != bolt.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that deleting a bucket causes nested buckets to be deleted.
func TestBucket_DeleteBucket_Nested(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		foo, err := widgets.CreateBucket([]byte("foo"))
		if err != nil {
			t.Fatal(err)
		}

		bar, err := foo.CreateBucket([]byte("bar"))
		if err != nil {
			t.Fatal(err)
		}
		if err := bar.Put([]byte("baz"), []byte("bat")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Bucket([]byte("widgets")).DeleteBucket([]byte("foo")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that deleting a bucket causes nested buckets to be deleted after they have been committed.
func TestBucket_DeleteBucket_Nested2(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		foo, err := widgets.CreateBucket([]byte("foo"))
		if err != nil {
			t.Fatal(err)
		}

		bar, err := foo.CreateBucket([]byte("bar"))
		if err != nil {
			t.Fatal(err)
		}

		if err := bar.Put([]byte("baz"), []byte("bat")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		widgets := tx.Bucket([]byte("widgets"))
		if widgets == nil {
			t.Fatal("expected widgets bucket")
		}

		foo := widgets.Bucket([]byte("foo"))
		if foo == nil {
			t.Fatal("expected foo bucket")
		}

		bar := foo.Bucket([]byte("bar"))
		if bar == nil {
			t.Fatal("expected bar bucket")
		}

		if v := bar.Get([]byte("baz")); !bytes.Equal(v, []byte("bat")) {
			t.Fatalf("unexpected value: %v", v)
		}
		if err := tx.DeleteBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		if tx.Bucket([]byte("widgets")) != nil {
			t.Fatal("expected bucket to be deleted")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that deleting a child bucket with multiple pages causes all pages to get collected.
// NOTE: Consistency check in bolt_test.DB.Close() will panic if pages not freed properly.
func TestBucket_DeleteBucket_Large(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		foo, err := widgets.CreateBucket([]byte("foo"))
		if err != nil {
			t.Fatal(err)
		}

		for i := 0; i < 1000; i++ {
			if err := foo.Put([]byte(fmt.Sprintf("%d", i)), []byte(fmt.Sprintf("%0100d", i))); err != nil {
				t.Fatal(err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a simple value retrieved via Bucket() returns a nil.
func TestBucket_Bucket_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if err := widgets.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if b := tx.Bucket([]byte("widgets")).Bucket([]byte("foo")); b != nil {
			t.Fatal("expected nil bucket")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that creating a bucket on an existing non-bucket key returns an error.
func TestBucket_CreateBucket_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}

		if err := widgets.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if _, err := widgets.CreateBucket([]byte("foo")); err != bolt.ErrIncompatibleValue {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that deleting a bucket on an existing non-bucket key returns an error.
func TestBucket_DeleteBucket_IncompatibleValue(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := widgets.Put([]byte("foo"), []byte("bar")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Bucket([]byte("widgets")).DeleteBucket([]byte("foo")); err != bolt.ErrIncompatibleValue {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure bucket can set and update its sequence number.
func TestBucket_Sequence(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		bkt, err := tx.CreateBucket([]byte("0"))
		if err != nil {
			t.Fatal(err)
		}

		// Retrieve sequence.
		if v := bkt.Sequence(); v != 0 {
			t.Fatalf("unexpected sequence: %d", v)
		}

		// Update sequence.
		if err := bkt.SetSequence(1000); err != nil {
			t.Fatal(err)
		}

		// Read sequence again.
		if v := bkt.Sequence(); v != 1000 {
			t.Fatalf("unexpected sequence: %d", v)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Verify sequence in separate transaction.
	if err := db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket([]byte("0")).Sequence(); v != 1000 {
			t.Fatalf("unexpected sequence: %d", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can return an autoincrementing sequence.
func TestBucket_NextSequence(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		widgets, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		woojits, err := tx.CreateBucket([]byte("woojits"))
		if err != nil {
			t.Fatal(err)
		}

		// Make sure sequence increments.
		if seq, err := widgets.NextSequence(); err != nil {
			t.Fatal(err)
		} else if seq != 1 {
			t.Fatalf("unexpecte sequence: %d", seq)
		}

		if seq, err := widgets.NextSequence(); err != nil {
			t.Fatal(err)
		} else if seq != 2 {
			t.Fatalf("unexpected sequence: %d", seq)
		}

		// Buckets should be separate.
		if seq, err := woojits.NextSequence(); err != nil {
			t.Fatal(err)
		} else if seq != 1 {
			t.Fatalf("unexpected sequence: %d", 1)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket will persist an autoincrementing sequence even if its
// the only thing updated on the bucket.
// https://github.com/boltdb/bolt/issues/296
func TestBucket_NextSequence_Persist(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.Bucket([]byte("widgets")).NextSequence(); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		seq, err := tx.Bucket([]byte("widgets")).NextSequence()
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		} else if seq != 2 {
			t.Fatalf("unexpected sequence: %d", seq)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that retrieving the next sequence on a read-only bucket returns an error.
func TestBucket_NextSequence_ReadOnly(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
			t.Fatal(err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		_, err := tx.Bucket([]byte("widgets")).NextSequence()
		if err != bolt.ErrTxNotWritable {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that retrieving the next sequence for a bucket on a closed database return an error.
func TestBucket_NextSequence_Closed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	b, err := tx.CreateBucket([]byte("widgets"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.NextSequence(); err != bolt.ErrTxClosed {
		t.Fatal(err)
	}
}

// Ensure a user can loop over all key/value pairs in a bucket.
func TestBucket_ForEach(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("0000")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("baz"), []byte("0001")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("bar"), []byte("0002")); err != nil {
			t.Fatal(err)
		}

		var index int
		if err := b.ForEach(func(k, v []byte) error {
			switch index {
			case 0:
				if !bytes.Equal(k, []byte("bar")) {
					t.Fatalf("unexpected key: %v", k)
				} else if !bytes.Equal(v, []byte("0002")) {
					t.Fatalf("unexpected value: %v", v)
				}
			case 1:
				if !bytes.Equal(k, []byte("baz")) {
					t.Fatalf("unexpected key: %v", k)
				} else if !bytes.Equal(v, []byte("0001")) {
					t.Fatalf("unexpected value: %v", v)
				}
			case 2:
				if !bytes.Equal(k, []byte("foo")) {
					t.Fatalf("unexpected key: %v", k)
				} else if !bytes.Equal(v, []byte("0000")) {
					t.Fatalf("unexpected value: %v", v)
				}
			}
			index++
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if index != 3 {
			t.Fatalf("unexpected index: %d", index)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure a database can stop iteration early.
func TestBucket_ForEach_ShortCircuit(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("bar"), []byte("0000")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("baz"), []byte("0000")); err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), []byte("0000")); err != nil {
			t.Fatal(err)
		}

		var index int
		if err := tx.Bucket([]byte("widgets")).ForEach(func(k, v []byte) error {
			index++
			if bytes.Equal(k, []byte("baz")) {
				return errors.New("marker")
			}
			return nil
		}); err == nil || err.Error() != "marker" {
			t.Fatalf("unexpected error: %s", err)
		}
		if index != 2 {
			t.Fatalf("unexpected index: %d", index)
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that looping over a bucket on a closed database returns an error.
func TestBucket_ForEach_Closed(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	b, err := tx.CreateBucket([]byte("widgets"))
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := b.ForEach(func(k, v []byte) error { return nil }); err != bolt.ErrTxClosed {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that an error is returned when inserting with an empty key.
func TestBucket_Put_EmptyKey(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte(""), []byte("bar")); err != bolt.ErrKeyRequired {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := b.Put(nil, []byte("bar")); err != bolt.ErrKeyRequired {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that an error is returned when inserting with a key that's too large.
func TestBucket_Put_KeyTooLarge(t *testing.T) {
	db := MustOpenDB()
	defer db.MustClose()
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put(make([]byte, 32769), []byte("bar")); err != bolt.ErrKeyTooLarge {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that an error is returned when inserting a value that's too large.
func TestBucket_Put_ValueTooLarge(t *testing.T) {
	// Skip this test on DroneCI because the machine is resource constrained.
	if os.Getenv("DRONE") == "true" {
		t.Skip("not enough RAM for test")
	}

	db := MustOpenDB()
	defer db.MustClose()

	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Put([]byte("foo"), make([]byte, bolt.MaxValueSize+1)); err != bolt.ErrValueTooLarge {
			t.Fatalf("unexpected error: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure that a bucket can write random keys and values across multiple transactions.
func TestBucket_Put_Single(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	index := 0
	if err := quick.Check(func(items testdata) bool {
		db := MustOpenDB()
		defer db.MustClose()

		m := make(map[string][]byte)

		if err := db.Update(func(tx *bolt.Tx) error {
			if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
				t.Fatal(err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		for _, item := range items {
			if err := db.Update(func(tx *bolt.Tx) error {
				if err := tx.Bucket([]byte("widgets")).Put(item.Key, item.Value); err != nil {
					t.Fatal("put error: " + err.Error())
				}
				m[string(item.Key)] = item.Value
				return nil
			}); err != nil {
				t.Fatal(err)
			}

			// Verify all key/values so far.
			if err := db.View(func(tx *bolt.Tx) error {
				i := 0
				for k, v := range m {
					value := tx.Bucket([]byte("widgets")).Get([]byte(k))
					if !bytes.Equal(value, v) {
						t.Logf("value mismatch [run %d] (%d of %d):\nkey: %x\ngot: %x\nexp: %x", index, i, len(m), []byte(k), value, v)
						db.CopyTempFile()
						t.FailNow()
					}
					i++
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}

		index++
		return true
	}, nil); err != nil {
		t.Error(err)
	}
}

// Ensure that a transaction can insert multiple key/value pairs at once.
func TestBucket_Put_Multiple(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	if err := quick.Check(func(items testdata) bool {
		db := MustOpenDB()
		defer db.MustClose()

		// Bulk insert all values.
		if err := db.Update(func(tx *bolt.Tx) error {
			if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
				t.Fatal(err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				if err := b.Put(item.Key, item.Value); err != nil {
					t.Fatal(err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// Verify all items exist.
		if err := db.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				value := b.Get(item.Key)
				if !bytes.Equal(item.Value, value) {
					db.CopyTempFile()
					t.Fatalf("exp=%x; got=%x", item.Value, value)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		return true
	}, qconfig()); err != nil {
		t.Error(err)
	}
}

// Ensure that a transaction can delete all key/value pairs and return to a single leaf page.
func TestBucket_Delete_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	if err := quick.Check(func(items testdata) bool {
		db := MustOpenDB()
		defer db.MustClose()

		// Bulk insert all values.
		if err := db.Update(func(tx *bolt.Tx) error {
			if _, err := tx.CreateBucket([]byte("widgets")); err != nil {
				t.Fatal(err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		if err := db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte("widgets"))
			for _, item := range items {
				if err := b.Put(item.Key, item.Value); err != nil {
					t.Fatal(err)
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		// Remove items one at a time and check consistency.
		for _, item := range items {
			if err := db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("widgets")).Delete(item.Key)
			}); err != nil {
				t.Fatal(err)
			}
		}

		// Anything before our deletion index should be nil.
		if err := db.View(func(tx *bolt.Tx) error {
			if err := tx.Bucket([]byte("widgets")).ForEach(func(k, v []byte) error {
				t.Fatalf("bucket should be empty; found: %06x", trunc(k, 3))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}

		return true
	}, qconfig()); err != nil {
		t.Error(err)
	}
}

func ExampleBucket_Put() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Start a write transaction.
	if err := db.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}

		// Set the value "bar" for the key "foo".
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Read value back in a different read-only transaction.
	if err := db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		fmt.Printf("The value of 'foo' is: %s\n", value)
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// The value of 'foo' is: bar
}

func ExampleBucket_Delete() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Start a write transaction.
	if err := db.Update(func(tx *bolt.Tx) error {
		// Create a bucket.
		b, err := tx.CreateBucket([]byte("widgets"))
		if err != nil {
			return err
		}

		// Set the value "bar" for the key "foo".
		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
			return err
		}

		// Retrieve the key back from the database and verify it.
		value := b.Get([]byte("foo"))
		fmt.Printf("The value of 'foo' was: %s\n", value)

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Delete the key in a different write transaction.
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("widgets")).Delete([]byte("foo"))
	}); err != nil {
		log.Fatal(err)
	}

	// Retrieve the key again.
	if err := db.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("widgets")).Get([]byte("foo"))
		if value == nil {
			fmt.Printf("The value of 'foo' is now: nil\n")
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// The value of 'foo' was: bar
	// The value of 'foo' is now: nil
}

func ExampleBucket_ForEach() {
	// Open the database.
	db, err := bolt.Open(tempfile(), 0666, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(db.Path())

	// Insert data into a bucket.
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("animals"))
		if err != nil {
			return err
		}

		if err := b.Put([]byte("dog"), []byte("fun")); err != nil {
			return err
		}
		if err := b.Put([]byte("cat"), []byte("lame")); err != nil {
			return err
		}
		if err := b.Put([]byte("liger"), []byte("awesome")); err != nil {
			return err
		}

		// Iterate over items in sorted key order.
		if err := b.ForEach(func(k, v []byte) error {
			fmt.Printf("A %s is %s.\n", k, v)
			return nil
		}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close database to release file lock.
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	// Output:
	// A cat is lame.
	// A dog is fun.
	// A liger is awesome.
}
