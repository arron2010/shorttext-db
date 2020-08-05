package utils

import (
	"sort"
)

type Float32Desc []float32

func (p Float32Desc) Len() int           { return len(p) }
func (p Float32Desc) Less(i, j int) bool { return p[i] > p[j] || isNaN(p[i]) && !isNaN(p[j]) }
func (p Float32Desc) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Float32Desc) Sort()              { sort.Sort(p) }

func isNaN(f float32) bool {
	return f != f
}

func SortArrayInt(origin []int) {
	sort.SliceStable(origin, func(i, j int) bool {
		return origin[i] < origin[j]
	})
}

//func SortFloat32Desc(orgin []float32) {
//	sort.SliceStable(origin, func(i, j float32) bool {
//		return origin[i] < origin[j]
//	})
//}

func SortArrayIntReverse(origin []int) {
	sort.SliceStable(origin, func(i, j int) bool {
		return origin[i] > origin[j]
	})
}
