package main

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/memkv"
	"github.com/xp/shorttext-db/utils"
)

func main() {
	config.LoadSettings("/opt/test/config/test_case3.txt", nil)
	server := memkv.NewDBServer()
	server.Start()
	utils.WaitFor()
}
