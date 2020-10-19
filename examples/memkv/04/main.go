package main

import (
	"fmt"
	"github.com/xp/shorttext-db/memkv/proto"
	"reflect"
)

func main() {
	//encoder :=&task.MessageEncoder{NewMsgSerializer()}
	source := &proto.DbQueryParam{}

	name := reflect.TypeOf(source).String()
	fmt.Println(name)
}
