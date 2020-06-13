package main

import (
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/filedb"
	"github.com/xp/shorttext-db/shardeddb"
)

type User struct {
	Id   int
	Name string
}

func test01() {
	p, err := filedb.NewSequenceProxy("127.0.0.1:7892")
	if err != nil {
		fmt.Println(err)
		return
	}
	n := p.Next()
	fmt.Println(n)
}

func test02() {
	config.LoadSettings("/opt/test/config/test_case1.txt")

	kv, _ := shardeddb.NewKVStore("testdb")
	user := &User{Id: 1, Name: "AAA"}
	for i := 1; i <= 45; i++ {
		_, k := kv.Set(0, user)
		fmt.Println(k)
	}

	//user1 := &User{}
	//kv.Get(0,user1)

}

func test03() {
	s := filedb.NewSequence(0)
	s.SetStart(0)
	s.Close()
}
func main() {
	test02()
}
