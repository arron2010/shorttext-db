package config

import (
	"encoding/json"
	"github.com/xp/shorttext-db/utils"
	"strconv"
	"testing"
)

func TestCreateCaseInfo(t *testing.T) {
	cards := make([]*Card, 0, 0)
	for i := 1; i <= 3; i++ {
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
		utils.WriteFile(path, string(buff))
	}
	configObj := &Config{}
	configObj.KVDBNames = []string{"testdb"}
	configObj.KVDBFilePath = "/opt/data"
	configObj.KVIdleTimeout = 120
	configObj.KVTimeout = 120
	configObj.KVServerAddr = "127.0.0.1:5009"
	configObj.KVDBMaxRange = 3
	configObj.KVDBRowCount = 5
	configObj.SequenceServer = "127.0.0.1:7892"
	configObj.KVDRowStart = 1
	configObj.DictPath = "/opt/gopath/bin/dict/jieba.dict.utf8"
	configObj.HmmPath = "/opt/gopath/bin/dict/hmm_model.utf8"
	configObj.UserDictPath = "/opt/gopath/bin/dict/user.dict.utf8"
	configObj.IdfPath = "/opt/gopath/bin/dict/idf.utf8"
	configObj.StopWordsPath = "/opt/gopath/bin/dict/stop_words.utf8"

	buff, _ := json.Marshal(configObj)
	utils.WriteFile(configPath, string(buff))

}
