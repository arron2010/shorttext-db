package shardeddb

import (
	"bufio"
	"com.neep/goplatform/util"
	"encoding/json"
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/entities"
	"github.com/xp/shorttext-db/gjson"
	"github.com/xp/shorttext-db/shardedkv"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"
)

type User struct {
	Id   int
	Name string
}

func TestJsonMarshal(t *testing.T) {
	user := &User{Name: "AAA", Id: 1}
	buff, err := json.Marshal(user)
	if err != nil {
		fmt.Println(err)
	}
	text := util.BytesToString(buff)
	fmt.Println(text)
	var obj shardedkv.Object
	json.Unmarshal(buff, &obj)
	fmt.Println(obj)
}

func TestDbNodeClient_Set(t *testing.T) {
	client := &dbNodeClient{}
	client.generateId()
}

type goods struct {
	Id    string
	_desc string
}

var goodsItems = []goods{
	{"101", `金属套玻璃管温度计\WNY-11\150mm`},
	{"102", `金属套玻璃管温度计\WNY-12\158mm`},
	{"103", `弹簧\65.378.003/钢丝6/钢丝11\哈汽\国产`},
	{"104", `弹簧\65.378.005/钢丝6/钢丝11\哈汽\国产`},
	//{"2", `酸度计\HGY-2018\0-14pH\非防爆型\IP67\国产\核工业北京化工冶金研究院\台`},
	//{"3", `三相电压变送器\FPVX-V1-F1-P2-O3\国产\台`},
}

func TestGetDBNode(t *testing.T) {
	//dbNode,err := NewDBNode()
	//if err != nil{
	//	panic(err)
	//}
	//mm :=dbNode.GetMemStorage("testdb")
	//for i:=0;i <
}

func TestKeywordIndex_Create(t *testing.T) {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	index := NewIndex()
	for i := 0; i < len(goodsItems); i++ {
		err := index.Create(goodsItems[i]._desc, goodsItems[i].Id)
		if err != nil {
			fmt.Println(err)
		}
	}
	var keywrods = []string{`金属`, `弹簧`}

	keyWords := make([]config.Text, 0)
	kwLen := 0
	for _, v := range keywrods {
		l := utf8.RuneCountInString(v)
		keyWords = append(keyWords, config.Text(v))
		kwLen = kwLen + l
	}
	found, _ := index.Find(keyWords, kwLen)
	fmt.Println(found)

}

var index Index

var record *entities.Record
var kwWords []config.Text
var kwLen int
var db IMemStorage

func TestMain(m *testing.M) {
	config.LoadSettings("/opt/test/config/test_case1.txt")

	//inputText := `弹簧\136.318.004/φ100/CZK50-9.3/4.2\哈汽\国产`
	//inputText :=`无源核子料位计\HVZR-TP01-2SV-AC\0-8000mm\开关量`
	//index = createIndex()
	//p := parse.NewParser()
	//words, _ := p.Parse(inputText)
	//kwWords,kwLen = createRecord(words)
	Start(false)
	db = GetDBNode().GetMemStorage("testdb_1")
	loadData(10000)
	m.Run()
}

func BenchmarkKeywordIndex_Find(b *testing.B) {

	b.ResetTimer()
	b.StartTimer()
	text := `电压变送器\DC0-99mV DC4-20mA DC220V FPD-1\国产`

	for i := 0; i < b.N; i++ {
		db.Find(text)
		//index.Find(kwWords,kwLen)
	}
	b.StopTimer()
}

func createRecord(words []config.Text) ([]config.Text, int) {
	//r := &entities.Record{}
	kwLen := 0
	strList := make([]string, 0)
	for _, v := range words {
		kwLen = kwLen + len(v)
		strList = append(strList, string(v))
	}
	//	fmt.Println(strings.Join(strList,"|"))
	return words, kwLen
}

func createIndex() Index {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	var path = `/opt/test/采购数据0123.txt`
	f, err := os.Open(path)
	buf := bufio.NewReader(f)
	if err != nil {
		fmt.Println("读取文件失败：", path)
	}
	index := NewIndex()
	var count = 0
	for {
		line, _, err := buf.ReadLine()
		if err != nil {
			if err != io.EOF {
				fmt.Println("读取文件内容错误:", err)
				break
			}
			break
		}
		if !utf8.Valid(line) {
			continue
		}
		lineStr := string(line)
		segments := strings.Split(lineStr, "\\")
		sliceSeg := segments[1:]
		text := strings.Join(sliceSeg, "\\")
		err = index.Create(text, segments[0])
		if err != nil {
			fmt.Println("索引创建失败:", err)
		}
		count++
	}
	fmt.Println("创建成功，记录数:", count)
	return index
}

func loadData(maxCount int) Index {
	var record *entities.Record
	var path = `/opt/test/采购数据0123.txt`
	f, err := os.Open(path)
	buf := bufio.NewReader(f)
	if err != nil {
		fmt.Println("读取文件失败：", path)
	}
	//tpl:=`{"id":"%s","desc":"%s"}`
	var count = 0
	for {

		line, _, err := buf.ReadLine()
		if err != nil {
			if err != io.EOF {
				fmt.Println("读取文件内容错误:", err)
				break
			}
			break
		}
		if !utf8.Valid(line) {
			continue
		}

		if count >= maxCount {
			break
		}
		lineStr := string(line)
		segments := strings.Split(lineStr, "\\")
		sliceSeg := segments[1:]
		text := strings.Join(sliceSeg, "\\")

		record = &entities.Record{Id: segments[0], Desc: text}
		buf, err := json.Marshal(record)
		jsonText := string(buf)
		if gjson.Valid(jsonText) {
			err = db.SetWithIndex(strconv.Itoa(count), jsonText, config.GJSON_FIELD_DESC)
			if err != nil {
				fmt.Println("索引创建失败:", err)
			}
		} else {
			fmt.Println("非gjson格式:", lineStr)
		}

		count++

	}
	fmt.Println("创建成功，记录数:", count)
	return index
}

func TestStart(t *testing.T) {
	text := `电压变送器\DC0-99mV DC4-20mA DC220V FPD-1\国产`
	records, err := db.Find(text)
	if err != nil {
		fmt.Println("TestStart 发生错误:", err)
	}
	fmt.Println(records)
}
