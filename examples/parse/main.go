package main

import (
	"bufio"
	"fmt"
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/entities"
	"github.com/xp/shorttext-db/parse"
	"github.com/xp/shorttext-db/shardeddb"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"
)

func createIndex() shardeddb.Index {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	var path = `/opt/test/物料搜索_10万条.txt`
	f, err := os.Open(path)
	buf := bufio.NewReader(f)
	if err != nil {
		fmt.Println("读取文件失败：", path)
	}
	index := shardeddb.NewIndex()
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
		sliceSeg := segments[3:]
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

func createRecord(words []config.Text) *entities.Record {
	r := &entities.Record{}
	r.KeyWords = words
	strList := make([]string, 0)
	for _, v := range words {
		r.KWLength = r.KWLength + len(v)
		strList = append(strList, string(v))
	}
	fmt.Println(strings.Join(strList, "|"))
	return r
}
func main() {
	index := createIndex()
	inputText := `弹簧\136.318.004/φ100/CZK50-9.3/4.2\哈汽\国产`
	p := parse.NewParser()
	words, _ := p.Parse(inputText)
	records := createRecord(words)
	begin := time.Now()
	for i := 1; i < 1000; i++ {
		index.Find(records)

	}
	elapsed := time.Since(begin)
	fmt.Printf("耗费时间:%.2f毫秒\n", elapsed.Seconds()*1000)
	time.Sleep(10 * time.Second)
}
