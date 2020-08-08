package main

import (
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/filedb"
	"github.com/xp/shorttext-db/shardeddb"
	"strconv"
)

type User struct {
	Id   int
	Name string
}

//func test01() {
//	p, err := filedb.NewSequenceProxy("127.0.0.1:7892")
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//	n := p.Next()
//	fmt.Println(n)
//}

func test02() {
	config.LoadSettings("/opt/test/config/test_case1.txt", nil)

	kv, _ := shardeddb.NewKVStore("testdb")

	for i := 1; i <= 3; i++ {
		value := "AAA" + strconv.Itoa(i)
		user := &User{Id: 1, Name: value}
		_, k := kv.Set(0, user)
		fmt.Println(k)
	}
	//kv.IniSeq(0)
	val := kv.Next()
	fmt.Println(val)
	//user1 := &User{}
	//kv.Get(135,user1)
	//fmt.Println(user1)

}
func test05() {
	config.LoadSettings("/opt/test/config/test_case1.txt", nil)

	kv, _ := shardeddb.NewKVStore("testdb")
	user1 := &User{}
	kv.Get(1, user1)
	fmt.Println(user1)
}
func test03() {
	s := filedb.NewSequence(0)
	s.SetStart("testdb", 0)
	//	fmt.Println(s.Next("testdb"))
	s.Close()
}
func main() {
	test05()
}
