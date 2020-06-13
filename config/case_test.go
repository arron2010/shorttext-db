package config

import (
	"com.neep/goplatform/util"
	"encoding/json"
	"strconv"
	"testing"
)

func TestCreateCaseInfo(t *testing.T) {
	cards := make([]*Card, 0, 0)
	for i := 1; i <= 4; i++ {
		card := &Card{ID: uint64(i), Name: "test" + strconv.Itoa(i), IP: "127.0.0.1", Port: 2000 + uint32(i), Alive: true}
		cards = append(cards, card)
	}

	for i := 1; i <= len(cards); i++ {
		caseInfo := &Case{}
		caseInfo.Local = cards[i-1]
		caseInfo.CaseId = "test"
		caseInfo.CardList = cards[1:]
		caseInfo.MasterCard = cards[0]
		buff, _ := json.Marshal(caseInfo)
		path := "/opt/test/config/test_case" + strconv.Itoa(i) + ".txt"
		util.WriteFile(path, string(buff))
	}
	configObj := &Config{}
	configObj.KVDBNames = []string{"testdb"}
	configObj.KVDBFilePath = "/opt/data"
	configObj.KVIdleTimeout = 120
	configObj.KVTimeout = 120
	configObj.KVServerAddr = "127.0.0.1:5009"
	configObj.KVDBMaxRange = 3
	configObj.KVDBRowCount = 15
	configObj.SequenceServer = "127.0.0.1:7892"
	configObj.KVDRowStart = 1
	buff, _ := json.Marshal(configObj)
	util.WriteFile(configPath, string(buff))

}
