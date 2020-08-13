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
	parse.IParse
	Create(prefix string, key string) error
	Find(keyWords []config.Text, length int) (config.RatioSet, error)
}

func NewIndex() Index {
	k := &keywordIndex{}
	k.parser = parse.NewParser()
	k.dictionary = trie.NewTrie()
	return k
}

type keywordIndex struct {
	parser     parse.IParse
	dictionary *trie.Trie
	ratio      float32
}

/*
 根据关键字查找记录，并记录命中关键字的utf8字符长度
*/
func (k *keywordIndex) findOriginalItems(keyWords []config.Text) config.TextSet {
	result := make(config.TextSet)
	for _, word := range keyWords {
		dataItem, found := k.dictionary.Find(trie.Prefix(word))
		item := dataItem.(map[string]bool)
		if found {
			for itemKey, flag := range item {
				//当关键字关联的记录ID为无效状态的时候，直接忽略
				if !flag {
					continue
				}
				w, ok := result[itemKey]
				if ok {
					result[itemKey] = w + len(word)
				} else {
					result[itemKey] = len(word)
				}
			}
		}
	}
	return result
}

/*
关键字命中超过50%的记录，被提取出来。
*/
func (k *keywordIndex) Find(keyWords []config.Text, length int) (config.RatioSet, error) {
	orginalItems := k.findOriginalItems(keyWords)
	result := make(config.RatioSet)
	for k, v := range orginalItems {
		ratio := float32(v) / float32(length)
		if ratio < 0.5 {
			continue
		}
		result[k] = ratio
	}
	return result, nil
}

/*
创建索引
*/
func (k *keywordIndex) Create(prefix string, key string) error {
	parsed, err := k.parser.Parse(prefix)
	if err != nil {
		return err
	}
	//先删除已存在数据主键
	k.dictionary.DelItem(key)

	//建立关键字与数据主键的关联
	for _, v := range parsed {
		k.dictionary.Append(trie.Prefix(v), key, true)
	}

	return nil
}

func (k *keywordIndex) Parse(text string) ([]config.Text, error) {
	return k.parser.Parse(text)
}
