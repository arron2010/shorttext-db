package main

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/memkv"
	server2 "github.com/xp/shorttext-db/server"
	"github.com/xp/shorttext-db/utils"
)

func main() {
	config.LoadSettings("/opt/test/config/test_case2.txt", nil)
	memkv.NewDBServer(server2.GetNode())

	utils.WaitFor()
}
