package shardeddb

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/parse"
	"github.com/xp/shorttext-db/trie"
)

/*
 倒排索引接口，用来创建索引和查找关键字
*/
type Index interface {
	Create(prefix string, key string) error
	Find(keyword string) (config.TextSet, error)
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
}

func (k *KeywordIndex) Find(keyword string) (config.TextSet, error) {
	trieResult := k.dictionary.FindItems(keyword)
	result := make(config.TextSet)
	for _, v := range trieResult {
		result[v.Key] = true
	}
	return result, nil
}

func (k *KeywordIndex) Create(prefix string, key string) error {
	parsed, err := k.parser.Parse(prefix)
	if err != nil {
		return err
	}
	for _, v := range parsed {
		nodeItem := &trie.NodeItem{Key: key}
		k.dictionary.Set(trie.Prefix(v), trie.Item(nodeItem))
	}
	return nil
}
