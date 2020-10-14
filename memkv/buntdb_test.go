package memkv

import (
	"bytes"
	"fmt"
	"github.com/tidwall/gjson"
	"testing"
	"time"
)

type Timer struct {
	begin time.Time
	end   time.Time
}

func NewTimer() *Timer {
	t := &Timer{}
	t.Start()
	return t
}
func (t *Timer) Start() {
	t.begin = time.Now()
}

func (t *Timer) Stop() float64 {
	elapsed := time.Since(t.begin)
	return elapsed.Seconds() * 1000
}

func TestIndexString(t *testing.T) {
	//db, _ := Open(":memory:")
	////db.CreateIndex("name", "*", IndexInt)
	//db.Update(func(tx *Tx) error {
	//	tx.Set("8", "Tom", nil)
	//	tx.Set("2", "Janet", nil)
	//	tx.Set("3", "Carol", nil)
	//	tx.Set("4", "Alan", nil)
	//	tx.Set("5", "Sam", nil)
	//	tx.Set("9", "Melinda", nil)
	//	return nil
	//})
	//
	//db.View(func(tx *Tx) error {
	//	tx.AscendRange("name", "5", "8", func(key, value string) bool {
	//		fmt.Printf("%s: %s\n", key, value)
	//		return true
	//	})
	//	return nil
	//})

	//db.View(func(tx *Tx) error {
	//	tx.Ascend("name", func(key, value string) bool {
	//		fmt.Printf("%s: %s\n", key, value)
	//		return true
	//	})
	//	return nil
	//})

}

type Dimension struct {
	Dim1  uint64
	Dim2  string
	Dim3  string
	Dim4  string
	Dim5  string
	Value float64
}

func IndexJSON2(path string) func(a, b string) bool {
	return func(a, b string) bool {
		v1 := gjson.Get(a, path)
		v2 := gjson.Get(b, path)
		if v1.Str == v2.Str {
			return true
		} else {
			return false
		}
		r := gjson.Get(a, path).Less(gjson.Get(b, path), true)
		return r
	}
}

var gDB *DB

func TestDB_Indexes09(t *testing.T) {

	//gDB, _ = Open(":memory:")
	//const NUM = 10
	//
	////gDB.CreateIndex("Dim2_Dim3", "*", IndexJSON("Dim2"))
	//for i := 1; i <= NUM; i++ {
	//	k := uint64(i)
	//	item := &Dimension{}
	//	item.Dim1 = k
	//	item.Dim2 = "AAA" + strconv.Itoa(i)
	//
	//	val, _ := json.Marshal(item)
	//	//strVal := string(val)
	//	buf := make([]byte, 8)
	//	binary.BigEndian.PutUint64(buf, k)
	//	//strKey := string(buf)
	//	gDB.Put(&DbItem{key: buf, val: val})
	//}
	//temp := make([]byte, 8)
	//binary.BigEndian.PutUint64(temp, 1)
	//key1 := mvccEncode(temp, lockVer)
	//gDB.Put(&DbItem{key: key1, val: key1})
	//
	//temp = make([]byte, 8)
	//binary.BigEndian.PutUint64(temp, 1)
	//key2 := mvccEncode(temp, 3)
	//gDB.Put(&DbItem{key: key2, val: key2})
	//
	//temp = make([]byte, 8)
	//binary.BigEndian.PutUint64(temp, 1)
	//
	//result := gDB.Find(temp)
	////stop := make([]byte, 8)
	////binary.BigEndian.PutUint64(stop, 8)
	////result := gDB.Range(start,stop)
	//fmt.Println(result)
	////iterator := gDB.NewIterator(buf)
	//iterator.Next()
	//val := gDB.Get(buf)
	//fmt.Println(string(val))
	//fmt.Println(string(iterator.Value()))

	//_ = gDB.View(func(tx *Tx) error {
	//	buf := make([]byte,8)
	//	binary.BigEndian.PutUint64(buf,10)
	//	val,_ := tx.Get(buf)
	//	fmt.Println(string(val))
	//
	//	//iterator.Next()
	//	//iterator.Next()
	//	////val :=tx.FindFirstGreaterKey(Key(buf))
	//	//fmt.Println(string(iterator.Value()))
	//	return nil
	//})
	//gDB.Update(func(tx *Tx) error {
	//	tx.SetHitHandler("Dim2", func(scope map[interface{}]bool, item string) bool {
	//		//token :=gjson.Get(item,"Dim2")
	//		//val := token.Str
	//		//if len(val) ==0{
	//		//	return false
	//		//}
	//		_,ok := scope[item]
	//		return ok
	//	})
	//	return nil
	//})
	//val,_ := json.Marshal(items)
	//r := gjson.ParseBytes(val)
	//fmt.Println(r)
}

func TestDB_Indexes01(t *testing.T) {
	//testLoadData(t)
	//var err error
	//timer := NewTimer()
	////var result string
	//
	//	err = gDB.View(func(tx *Tx) error {
	//		err :=tx.SelectRange("Dim2","1,2,3,4",func(key, value string) bool {
	//			//token := gjson.Get(value,"Dim2")
	//			//if token.Str == "AAA9999"{
	//			//fmt.Printf("%s: %s\n", key, value)
	//			//}
	//
	//			return true
	//		})
	//		if err != nil{
	//			fmt.Println(err)
	//		}
	//		return nil
	//	})
	//	if err != nil{
	//		fmt.Println(err)
	//	}
	//
	//
	//fmt.Println(timer.Stop())

}
func TestDB_Indexes04(t *testing.T) {
	//testLoadData(t)
	//var err error
	//timer := NewTimer()
	////var result string
	//const NUM = 1
	////for i :=1;i <= NUM;i++{
	//val := `{"Dim2":"AAA1","Dim3":"BBB1"}`
	//err = gDB.View(func(tx *Tx) error {
	//	err := tx.AscendEqual("Dim2_Dim3", val, func(key, value string) bool {
	//
	//		fmt.Printf("%s: %s\n", key, value)
	//
	//		return true
	//	})
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//	return nil
	//})
	//if err != nil {
	//	fmt.Println(err)
	//}
	////}
	//fmt.Println(timer.Stop())

}

func TestDB_Indexes05(t *testing.T) {
	max := ^uint(0)
	fmt.Println(max)
	//testLoadData(t)
	//var err error
	//timer := NewTimer()
	////var result string
	//var count int
	//const NUM = 1 * 100
	//for i := 1; i <= NUM; i++ {
	//	val := fmt.Sprintf(`{"Dim2":"AAA%d"}`, i)
	//	err = gDB.View(func(tx *Tx) error {
	//		err := tx.AscendEqual("Dim2_Dim3", val, func(key , value string) bool {
	//
	//			count++
	//			//fmt.Printf("%s: %s\n", key, value)
	//			return true
	//		})
	//		if err != nil {
	//			fmt.Println(err)
	//		}
	//		return nil
	//	})
	//	if err != nil {
	//		fmt.Println(err)
	//	}
	//}
	//fmt.Println("时间花费:", timer.Stop())
	//fmt.Println("记录数量:", count)
}

func TestDB_Indexes03(t *testing.T) {
	a := []byte{1, 2, 3}
	b := []byte{1, 3}
	fmt.Println(bytes.Compare(a, b))
	//db, _ := Open(":memory:")
	//db.CreateIndex("last_name", "*", IndexJSON("name.last"))
	//db.CreateIndex("age", "*", IndexJSON("age"))
	//db.Update(func(tx *Tx) error {
	//	tx.Set("1", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, nil)
	//	tx.Set("2", `{"name":{"first":"Janet","last":"Johnson"},"age":47}`, nil)
	//	tx.Set("3", `{"name":{"first":"Carol","last":"Anderson"},"age":52}`, nil)
	//	tx.Set("4", `{"name":{"first":"Alan","last":"Cooper"},"age":28}`, nil)
	//	return nil
	//})
	//db.View(func(tx *Tx) error {
	//	fmt.Println("Order by last name")
	//	tx.AscendEqual("last_name", `{"name":{"first":"Tom","last":"Johnson"},"age":38}`, func(key, value string) bool {
	//		fmt.Printf("%s: %s\n", key, value)
	//		return true
	//	})
	//
	//	//tx.Ascend("last_name", func(key, value string) bool {
	//	//	fmt.Printf("%s: %s\n", key, value)
	//	//	return true
	//	//})
	//	return nil
	//})
}
