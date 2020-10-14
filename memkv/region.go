package memkv

type Region struct {
	Id    uint64
	Count int
	Full  bool
}

type RegionList []*Region

func NewRegionList() RegionList {
	r := make([]*Region, 0, 32)
	return RegionList(r)
}
func (r RegionList) Remove(regionId uint64) {
	var index int
	index = -1
	for i, item := range r {
		if item.Id == regionId {
			index = i
			break
		}
	}
	if index >= 0 {
		r = append(r[:index], r[index+1:]...)
	}
}

func (r RegionList) Add(e *Region) {
	r = append(r, e)
}

func (r RegionList) Get(index int) *Region {
	if index < 0 || index >= len(r) {
		return nil
	}
	return r[index]
}
