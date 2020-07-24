package shardeddb

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/entities"
	"github.com/xp/shorttext-db/parse"
	"github.com/xp/shorttext-db/trie"
	"unicode/utf8"
)

/*
 倒排索引接口，用来创建索引和查找关键字
*/
type Index interface {
	Create(prefix string, key string) error
	Find(record *entities.Record) (config.RatioSet, error)
}

func NewIndex() Index {
	k := &KeywordIndex{}
	k.parser = parse.NewParser()
	k.dictionary = trie.NewTrie()
	return k
}

type KeywordIndex struct {
	parser     parse.IParse
	dictionary *trie.Trie
	ratio      float32
}

/*
 根据关键字查找记录，并记录命中关键字的utf8字符长度
*/
func (k *KeywordIndex) findOriginalItems(record *entities.Record) config.TextSet {
	result := make(config.TextSet)
	for word, length := range record.KeyWords {
		item, found := k.dictionary.Find(trie.Prefix(word))
		itemKey := item.Key
		if found {
			w, ok := result[itemKey]
			if ok {
				result[itemKey] = w + length
			} else {
				result[itemKey] = length
			}
		}
	}
	return result
}

func (k *KeywordIndex) Find(record *entities.Record) (config.RatioSet, error) {
	orginalItems := k.findOriginalItems(record)
	result := make(config.RatioSet)
	for k, v := range orginalItems {
		ratio := float32(v) / float32(record.KWLength)
		if ratio < 0.5 {
			continue
		}
		result[k] = ratio
	}
	return result, nil
}

func (k *KeywordIndex) Create(prefix string, key string) error {
	parsed, err := k.parser.Parse(prefix)
	if err != nil {
		return err
	}
	for _, v := range parsed {
		nodeItem := &trie.NodeItem{Key: key, Weight: utf8.RuneCountInString(prefix)}
		k.dictionary.Set(trie.Prefix(v), trie.Item(nodeItem))
	}
	return nil
}
