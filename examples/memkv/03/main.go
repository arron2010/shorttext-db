package main

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/memkv"
	server2 "github.com/xp/shorttext-db/server"
	"github.com/xp/shorttext-db/utils"
)

func main() {
	config.LoadSettings("/opt/test/config/test_case3.txt", nil)
	//easymr.Run()
	collaborator.GetCollaborator()
	memkv.NewDBServer(server2.GetNode())

	utils.WaitFor()
}
