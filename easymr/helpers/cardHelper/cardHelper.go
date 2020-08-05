package cardHelper

import (
	"github.com/xp/shorttext-db/easymr/artifacts/card"
	"github.com/xp/shorttext-db/glogger"
)

var logger = glogger.MustGetLogger("cardHelper")

func UnmarshalCards(original []interface{}) []card.Card {
	var cards []card.Card
	for _, o := range original {
		oo := o.(map[string]interface{})

		var (
			api  string = ""
			seed bool   = false
		)

		if oo["api"] != nil {
			api = oo["api"].(string)
		}

		if oo["seed"] != nil {
			seed = oo["seed"].(bool)
		}

		cards = append(cards, card.Card{oo["ip"].(string), int32(oo["port"].(float64)), oo["alive"].(bool), api, seed})
	}
	return cards
}

func RangePrint(cards map[string]*card.Card) {
	for _, c := range cards {
		var (
			alive string
			seed  string
		)
		if c.Alive {
			alive = "Alive"
		} else {
			alive = "Terminated"
		}

		if c.IsSeed() {
			seed = "Seed"
		} else {
			seed = "Non-Seed"
		}
		logger.Info(c.GetFullIP(), alive, seed)
	}
}
