package entities

import "github.com/xp/shorttext-db/config"

type Record struct {
	KeyWords []config.Text
	KWLength int
}
