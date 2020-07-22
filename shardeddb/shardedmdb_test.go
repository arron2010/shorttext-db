package shardeddb

import (
	"com.neep/goplatform/util"
	"encoding/json"
	"fmt"
	"github.com/xp/shorttext-db/shardedkv"
	"testing"
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
	{"2", `酸度计\HGY-2018\0-14pH\非防爆型\IP67\国产\核工业北京化工冶金研究院\台`},
	{"3", `三相电压变送器\FPVX-V1-F1-P2-O3\国产\台`},
}

func TestGetDBNode(t *testing.T) {
	//dbNode,err := NewDBNode()
	//if err != nil{
	//	panic(err)
	//}
	//mm :=dbNode.GetMemStorage("testdb")
	//for i:=0;i <
}
