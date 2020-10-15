package memkv

import (
	"fmt"
	"github.com/xp/shorttext-db/config"
	"strconv"
	"testing"
)

func TestChooser_UpdateRegion(t *testing.T) {
	ch := NewChooser()
	count := 1024 * 1024 * 1024 * 8
	for i := 1; i <= count; i++ {
		ch.mapper.Put(uint32(i), uint64(i))
	}
}
func TestChooser_Choose(t *testing.T) {
	ch := NewChooser()
	ch.mapper.maxRecords = 10
	cards := make([]*config.Card, 0, 0)
	for i := 1; i <= 5; i++ {
		cards = append(cards, &config.Card{ID: uint64(i)})
		if i == 2 || i == 5 {
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
