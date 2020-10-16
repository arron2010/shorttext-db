package main

import (
	"fmt"

	"github.com/xp/shorttext-db/bbolt"
	"os"
	"strconv"

	"time"
)

var fileMode os.FileMode = 0600 // owner can read and write
var db *bbolt.DB
var myBucket = []byte("test01")

func openDB() {

	database := `/opt/boltdb/test01.db`
	buckets := []string{"test01"}
	var err error
	// open the target file, file mode fileMode, and a 10 seconds timeout period
	db, err = bbolt.Open(database, fileMode, &bbolt.Options{Timeout: 10 * time.Second, PageSize: 100})
	if err != nil {
		fmt.Println(err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		for _, value := range buckets {
			_, err := tx.CreateBucketIfNotExists([]byte(value))
			if err != nil {
				return err
			}
		}
		return nil
	})

}
func testBucket_Put2() {
	openDB()
	const NUM = 10
	for i := 1; i <= NUM; i++ {
		key := strconv.Itoa(10000000 + i)
		bKey := []byte(key)
		db.Update(func(tx *bbolt.Tx) error {
			b := tx.Bucket(myBucket)
			bValue := []byte(`0123456701234567`)
			err := b.Put(bKey, bValue)
			if i == 5 {
				fmt.Println()
			}
			return err
		})
	}
}
func main() {
	testBucket_Put2()
}
