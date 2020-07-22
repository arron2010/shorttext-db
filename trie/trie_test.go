package trie

import (
	"fmt"
	"testing"
)

func TestTrie(t *testing.T) {
	//trie := NewTrie()
	//
	//trie.Insert(Prefix("ABC"), Item(&NodeItem{ID:1}))
	//
	//trie.Insert(Prefix("AEF"), Item(&NodeItem{ID:2}))
	//trie.Insert(Prefix("GHI"), Item(&NodeItem{ID:3}))
	//
	//
	//item, found := trie.Find(Prefix("ABC"))
	//if found && item != nil {
	//	fmt.Println(item)
	//}
}

func TestTrieChildren(t *testing.T) {
	trie := NewTrie()
	s1 := "1"
	trie.Insert(Prefix("AB"), Item(s1))

	s2 := "2"
	trie.Insert(Prefix("ABD2"), Item(&s2))

	s3 := "3"
	trie.Insert(Prefix("ABE3"), Item(&s3))

	s4 := "4"
	trie.Insert(Prefix("BAE3"), Item(&s4))

	result := trie.FindItems("AB")
	fmt.Println(result)

	//trie.VisitSubtree(Prefix("AB"), func(prefix Prefix, item Item) error {
	//	fmt.Println(string(prefix))
	//	return nil
	//})
	//path,found,leftover:= trie.findSubtreePath(Prefix("AB"))
	//
	//fmt.Println(len(path))
	//fmt.Println(found)
	//fmt.Println(leftover)
	//item, found := trie.Find(Prefix("ABC"))
	//if found && item != nil {
	//	fmt.Println(item)
	//}
}

func TestTrieChildren2(t *testing.T) {
	//trie := NewTrie()
	//
	//s1 := "1"
	//trie.Insert(Prefix("ABC1"), Item(&s1))
	//
	//trie.Insert(Prefix("ABD2"), Item(&NodeItem{ID:"2"}))
	//trie.Insert(Prefix("ABE3"), Item(&NodeItem{ID:"3"}))
	//
	//trie.Insert(Prefix("BAE3"), Item(&NodeItem{ID:"4"}))
	//
	//
	//trie.FindPrefixes(Prefix("AB"),func(prefix Prefix, item Item) error {
	//	fmt.Println("Prefix--->",string(prefix))
	//	fmt.Println("Item--->",item)
	//	return nil
	//})
	//	fmt.Println(result)

	//trie.VisitSubtree(Prefix("AB"), func(prefix Prefix, item Item) error {
	//	fmt.Println(string(prefix))
	//	return nil
	//})
	//path,found,leftover:= trie.findSubtreePath(Prefix("AB"))
	//
	//fmt.Println(len(path))
	//fmt.Println(found)
	//fmt.Println(leftover)
	//item, found := trie.Find(Prefix("ABC"))
	//if found && item != nil {
	//	fmt.Println(item)
	//}
}

func TestTrieItem(t *testing.T) {
	var s1 string
	var p1 *string
	s1 = "aaa"
	p1 = &s1
	*p1 = "bbb"
	fmt.Println(s1)
}
