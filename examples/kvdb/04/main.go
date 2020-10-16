package main

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/shardeddb"
	"github.com/xp/shorttext-db/utils"
)

func main() {
	config.LoadSettings("/opt/test/config/test_case4.txt", nil)
	node, err := shardeddb.NewDBNode(true)
	if err != nil {
		panic(err)
	}
	node.Start()
	utils.WaitFor()
}
