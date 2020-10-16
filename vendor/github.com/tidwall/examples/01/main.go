package main

import (
	"fmt"
	"github.com/tidwall/buntdb"
)

func testDb01(){
	var err error
	var db *buntdb.DB
	db, err = buntdb.Open(":memory:")//buntdb.Open("/opt/gopath/src/github.com/tidwall/examples/01/data.db")
	if err != nil {
		fmt.Println(err)
	}

	err = db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set("mykey", "myvalue", nil)
		return err
	})

	err = db.View(func(tx *buntdb.Tx) error {
		val, err := tx.Get("mykey")
		if err != nil{
			return err
		}
		fmt.Printf("value is %s\n", val)
		return nil
	})
	if err != nil{
		fmt.Println(err)
	}

	defer db.Close()
}

func main(){

	testDb01()
}

