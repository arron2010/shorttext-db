package memkv

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/network/proxy"
	"github.com/xp/shorttext-db/utils"
)

type chooser struct {
	currentRegions []uint32
	mapper         *RegionMapper
	masterId       uint32
	n              *proxy.NodeProxy
}

const MAX_REGION_COUNT = 64

func NewChooser() *chooser {
	c := &chooser{}
	c.mapper = NewRegionMapper(MAX_RECORD_COUNT)
	return c
}
func (c *chooser) GetMapper() *RegionMapper {
	return c.mapper
}

/*
对key的哈希值进行模运算，如果单个节点记录数量已经达到限制，就选择下一个节点
added为true 表示插入操作，进行区域选择
*/
func (c *chooser) Choose(key []byte, added bool) (uint64, uint32) {
	hashCode := utils.LeveldbHash(key)
	var choosed uint64
	if added {
		choosed = c.route(c.currentRegions, hashCode)
	} else {
		choosed = c.mapper.Get(hashCode)
	}
	//如果选择策略发生错误，就默认访问第2个节点
	if choosed == 0 {
		logger.Errorf("key:%v hash code:%d  added:%v 找不到合适的区域\n", key, hashCode, added)
		if added {
			choosed = 2
		}
	}
	return choosed, hashCode
}
func (c *chooser) route(regions []uint32, hashCode uint32) uint64 {
	count := uint32(len(regions) + 1)
	var r uint32
	for {
		if count <= 0 {
			break
		}
		r = hashCode % count
		if r == 0 {
			r = count
		}
		if r == c.masterId {
			hashCode = hashCode + 1
			continue
		}
		if c.mapper.IsAvailableRegion(r) && c.n.IsAlive(uint64(r)) {
			return uint64(r)
		} else {
			hashCode = hashCode + 1
		}
	}
	return uint64(r)
}
func (c *chooser) SetBuckets(cards []*config.Card) {
	c.currentRegions = make([]uint32, 0, len(cards))
	for _, card := range cards {
		if card.ID == proxy.MASTER_NODE_ID {
			continue
		}
		c.currentRegions = append(c.currentRegions, uint32(card.ID))
	}
	availabeRegions := c.mapper.GetAvailableRegion(c.currentRegions)
	if len(availabeRegions) == 0 {
		panic("各节点存储已满")
	}
}

/*
count < 0 表示删除数据，记录数减少
count > 0 表示插入数据，记录数增加
*/
func (c *chooser) UpdateRegion(regionId uint64, hashCode uint32, count int) {
	var successful bool
	if count > 0 {
		successful = c.mapper.Put(hashCode, regionId)
		logger.Infof("更新区域信息 HashCode[%d] RegionId[%d]\n", hashCode, regionId)
	}
	if count < 0 {
		successful = c.mapper.Del(hashCode)
	}
	if successful {
		c.mapper.SaveCount(uint32(regionId), count)
	}
}

func (c *chooser) Close() error {
	return c.mapper.Close()
}
