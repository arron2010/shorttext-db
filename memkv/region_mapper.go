package memkv

import (
	"encoding/binary"
	"github.com/xp/shorttext-db/bbolt/xfiledb"
)

type RegionMapper struct {
	keyDB      *xfiledb.DBWrapper
	regionDB   *xfiledb.DBWrapper
	maxRecords uint32
}

func NewRegionMapper(maxRecords uint32) *RegionMapper {
	mapper := &RegionMapper{}
	mapper.keyDB = xfiledb.NewDB("KeyDB")
	mapper.regionDB = xfiledb.NewDB("RegionDB")
	mapper.maxRecords = maxRecords
	var err error
	err = mapper.keyDB.Open()
	if err != nil {
		logger.Error("打开keyDB数据库报错:", err)
	}
	err = mapper.regionDB.Open()
	if err != nil {
		logger.Error("打开regionDB数据库报错:", err)
	}
	return mapper
}

func (r *RegionMapper) Get(hashCode uint32) uint64 {
	key := encode(hashCode)
	val := r.keyDB.Get(key)
	region := decode(val)
	return uint64(region)
}

func (r *RegionMapper) Del(hashCode uint32) bool {
	key := encode(hashCode)
	err := r.keyDB.Delete(key)
	if err != nil {
		return false
	}
	return true
}

func (r *RegionMapper) Put(hashCode uint32, regionId uint64) bool {
	key := encode(hashCode)
	val := encode(uint32(regionId))

	err := r.keyDB.Put(key, val)
	if err != nil {
		return false
	}
	return true
}

func (r *RegionMapper) SaveCount(regionId uint32, count int) {
	key := encode(regionId)
	var actual uint32 = 0
	val := r.regionDB.Get(key)
	actual = decode(val)

	if count > 0 {
		actual = actual + uint32(count)
	} else {
		if actual > 0 {
			actual = actual - uint32(count)
		}
	}
	if actual > 0 {
		r.regionDB.Put(key, encode(actual))
	}
}

func (r *RegionMapper) IsAvailableRegion(regionId uint32) bool {
	count := r.GetRegionCount(regionId)
	if count < r.maxRecords {
		return true
	}
	return false
}

func (r *RegionMapper) GetRegionCount(regionId uint32) uint32 {
	count := decode(r.regionDB.Get(encode(regionId)))
	return count
}

func (r *RegionMapper) GetAvailableRegion(regionIds []uint32) []uint32 {
	pairs := r.regionDB.GetAllKeyValues()
	if len(pairs) == 0 {
		return regionIds
	}
	current := make([]uint32, 0, len(regionIds))
	for _, regionId := range regionIds {
		if r.IsAvailableRegion(regionId) {
			current = append(current, regionId)
		}
	}
	return current
}

func (r *RegionMapper) Close() {
	r.regionDB.Close()
	r.keyDB.Close()
}
func encode(data uint32) []byte {
	buf := make([]byte, 4, 4)
	binary.BigEndian.PutUint32(buf, data)
	return buf
}

func decode(buf []byte) uint32 {
	if len(buf) == 0 && len(buf) != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(buf)
}
