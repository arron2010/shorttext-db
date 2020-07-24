package shardeddb

import (
	"com.neep/goplatform/util"
	"encoding/json"
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/entities"
	"github.com/xp/shorttext-db/shardedkv"
	"testing"
	"unicode/utf8"
)

type User struct {
	Id   int
	Name string
}

func TestJsonMarshal(t *testing.T) {
	user := &User{Name: "AAA", Id: 1}
	buff, err := json.Marshal(user)
	if err != nil {
		fmt.Println(err)
	}
	text := util.BytesToString(buff)
	fmt.Println(text)
	var obj shardedkv.Object
	json.Unmarshal(buff, &obj)
	fmt.Println(obj)
}

func TestDbNodeClient_Set(t *testing.T) {
	client := &dbNodeClient{}
	client.generateId()
}

type goods struct {
	Id    string
	_desc string
}

var goodsItems = []goods{
	{"1", `金属套玻璃管温度计\WNY-11\1℃\0-100℃\150mm\支`},
	//{"2", `酸度计\HGY-2018\0-14pH\非防爆型\IP67\国产\核工业北京化工冶金研究院\台`},
	//{"3", `三相电压变送器\FPVX-V1-F1-P2-O3\国产\台`},
}

func TestGetDBNode(t *testing.T) {
	//dbNode,err := NewDBNode()
	//if err != nil{
	//	panic(err)
	//}
	//mm :=dbNode.GetMemStorage("testdb")
	//for i:=0;i <
}

func TestKeywordIndex_Create(t *testing.T) {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	index := NewIndex()
	for i := 0; i < len(goodsItems); i++ {
		err := index.Create(goodsItems[i]._desc, goodsItems[i].Id)
		if err != nil {
			fmt.Println(err)
		}
	}
	var keywrods = []string{`金属`}
	r := &entities.Record{}
	r.KeyWords = make(map[string]int)
	for _, v := range keywrods {
		l := utf8.RuneCountInString(v)
		r.KeyWords[v] = l
		r.KWLength = r.KWLength + l
	}
	found, _ := index.Find(r)
	fmt.Println(found)

}
