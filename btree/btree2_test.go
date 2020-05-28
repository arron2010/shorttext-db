package btree

import (
	"fmt"
	"testing"
)

//type myItem uint32
//
//func (my myItem) Less(item Item, ctx interface{}) bool {
//	input := item.(myItem)
//	return my < input
//}
//C N G A H E K Q M F W L T Z D P R X Y S
func createBTree1() *BTree {
	mytree := New(2, "mytree")
	//users := []Int{1,2,3,4,5,6,7,8}
	//userItems :=items{}
	for i := 1; i <= 9; i++ {
		//userItems.insertAt(i,users[i])
		mytree.ReplaceOrInsert(Int(i))
	}
	//index,found :=userItems.find(&myItem{Key:280, Val: "AAA"}, struct {}{})
	//fmt.Printf("index:%d found:%v\n",index,found)
	return mytree
}

func TestBTree_Ascend(t *testing.T) {
	tree := createBTree1()

	PrintBTree(tree)
	//keys.Ascend(func(item Item) bool {
	//	fmt.Printf("%v \n", item)
	//	return true
	//})
}

func TestBTree_Find(t *testing.T) {
	source := []Int{200, 300, 400}
	itemsObj := items{}
	for i := 0; i < len(source); i++ {
		itemsObj.insertAt(i, source[i])
	}
	item := Int(100)
	index, found := itemsObj.find(item, struct{}{})
	fmt.Printf("index:%d found:%v\n", index, found)

}
