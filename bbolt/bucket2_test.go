package bbolt

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

var fileMode os.FileMode = 0600 // owner can read and write
var db *DB
var myBucket = []byte("test01")

func openDB() {

	database := `/opt/boltdb/test01.db`
	buckets := []string{"test01"}
	var err error
	// open the target file, file mode fileMode, and a 10 seconds timeout period
	db, err = Open(database, fileMode, &Options{Timeout: 10 * time.Second, PageSize: 100})
	if err != nil {
		fmt.Println(err)
	}
	err = db.Update(func(tx *Tx) error {
		for _, value := range buckets {
			_, err := tx.CreateBucketIfNotExists([]byte(value))
			if err != nil {
				return err
			}
		}
		return nil
	})

}
func TestBucket_Put2(t *testing.T) {
	openDB()
	const NUM = 10
	for i := 1; i <= NUM; i++ {
		key := strconv.Itoa(10000000 + i)
		bKey := []byte(key)
		t, err := db.Begin(true)
		if err != nil {
			fmt.Println(err)
		}
		b := t.Bucket(myBucket)

		bValue := []byte(`0123456701234567`)
		err = b.Put(bKey, bValue)
		if err != nil {
			fmt.Println(err)
		}
		err = t.Commit()
		if err != nil {
			fmt.Println(err)
		}
		t, _ = db.Begin(false)
		b1 := t.Bucket(myBucket)
		fmt.Println(string(b1.Get(bKey)))
	}
}
