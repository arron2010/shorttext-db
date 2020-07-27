package entities

type Record struct {
	Id          string  `json:"id"`
	Desc        string  `json:"desc"`
	PrefixRatio float32 `json:"-"`
}
