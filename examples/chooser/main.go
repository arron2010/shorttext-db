package main

import (
	"fmt"
	"github.com/xp/shorttext-db/memkv"
	"github.com/xp/shorttext-db/utils"
)

func main() {
	t := utils.NewTimer()
	fmt.Println("开始......")
	ch := memkv.NewChooser()
	count := 1024 * 1024 * 1024 * 8
	for i := 1; i <= count; i++ {
		ch.GetMapper().Put(uint32(i), uint64(i))
	}
	fmt.Println("插入操作执行完成:", t.Stop())
	t = utils.NewTimer()
	for i := 1024; i <= 2048; i++ {
		ch.GetMapper().Get(uint32(i))
	}
	fmt.Println("查询操作执行完成:", t.Stop())
}
