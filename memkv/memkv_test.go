package memkv

import (
	"fmt"
	"github.com/xp/shorttext-db/config"
	"strconv"
	"testing"
)

func TestChooser_Choose(t *testing.T) {
	ch := NewChooser()
	cards := make([]*config.Card, 0, 0)
	for i := 1; i <= 5; i++ {
		cards = append(cards, &config.Card{ID: uint64(i)})
		if i == 3 || i == 4 {
			continue
		}
		ch.mapper.SaveCount(uint32(i), 10)
	}
	ch.SetBuckets(cards)
	for i := 1; i <= 10; i++ {
		str := "AAA" + strconv.Itoa(i)
		regionId, hashCode := ch.Choose([]byte(str), true)
		fmt.Println(regionId, ":", hashCode)
		ch.mapper.Put(hashCode, regionId)
		newRegionId := ch.mapper.Get(hashCode)
		if regionId != newRegionId {
			t.Error(regionId, newRegionId)
		}
	}
	ch.Close()
}
