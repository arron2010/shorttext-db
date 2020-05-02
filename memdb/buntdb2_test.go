package memdb

import (
	"fmt"
	"github.com/xp/shorttext-db/gjson"
	"testing"
)

func TestIndexString(t *testing.T) {
	db, _ := Open(":memory:")
	db.CreateIndex("name", "*", IndexString)
	db.Update(func(tx *Tx) error {
		tx.Set("1", "Tom", nil)
		tx.Set("2", "Janet", nil)
		tx.Set("3", "Carol", nil)
		tx.Set("4", "Alan", nil)
		tx.Set("5", "Sam", nil)
		tx.Set("6", "Melinda", nil)
		return nil
	})

	db.View(func(tx *Tx) error {
		tx.AscendEqual("name", "AAA", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})
}

//11671678\发电专用设备及配件\汽机及辅助设备配件\汽机本体设备配件\螺栓\N733AP33044\GE\进口\件\020702\1
func TestJsonString(t *testing.T) {
	json1 := `{"text":"螺栓\\N733AP33044\\GE\\进口","classText":"发电专用设备及配件\汽机及辅助设备配件\汽机本体设备配件","unit":"件","classId":"020702","state":"1"}`
	r := gjson.Get(json1, "{text,classText}").Str

	fmt.Println(r)

}

func BenchmarkIndexJSON(b *testing.B) {
	b.ResetTimer()
	json1 := `{"text":"螺栓\\N733AP33044\\GE\\进口","classText":"发电专用设备及配件\汽机及辅助设备配件\汽机本体设备配件","unit":"件","classId":"020702","state":"1"}`

	for i := 0; i < b.N; i++ {
		_ = gjson.Get(json1, "{text}").Str
		//_ =gjson.Get(json1,"{classText}").Str
	}

	b.StopTimer()

}
