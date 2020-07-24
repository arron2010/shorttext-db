package parse

import (
	"fmt"
	"github.com/xp/shorttext-db/config"
	"strings"
	"testing"
)

func TestCutForChinese(t *testing.T) {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	cutter := NewCutter(config.GetConfig())
	strs := []string{`O型圈`, `90°弯管广州`, `ABCD弯管`, `EH供油装置HTGT300G`}
	for i := 0; i < len(strs); i++ {
		text := cutter.CutForHybrid(strs[i], 4)
		fmt.Printf("%d -- %s=>%s\n ", i+1, text, strings.Join(text, "|"))
	}

}

func Test_isASCII(t *testing.T) {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	cutter := NewCutter(config.GetConfig())
	r := cutter.isASCII(`弯管广州90°`)
	fmt.Println(r)
}

func Test_ParseReg(t *testing.T) {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	p := NewParser().(*Parser)
	s1 := `A,B C|D;E　F`
	strs := p.regSeparator.Split(s1, -1)
	fmt.Println(strings.Join(strs, "|"))
}

func Test_CutterReg(t *testing.T) {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	p := NewCutter(config.GetConfig())
	s1 := `φAB`
	fmt.Println(p.regReplaced.ReplaceAllString(s1, ""))
}

func TestJiebaCutter_CutForASCII(t *testing.T) {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	cutter := NewCutter(config.GetConfig())
	strs := []string{`520451-A1-02003`, `φAB`}
	for i := 0; i < len(strs); i++ {
		text := cutter.CutForASCII(strs[i], 4)
		fmt.Printf("%d -- %s=>%s\n ", i+1, strs[i], strings.Join(text, "|"))
	}
}

type parseCase struct {
	Text    string
	Results []string
}

var parseCaseEntity = []parseCase{
	{
		`汽水引出管\F002LGA015Z160L/φ159×18mm/20#\哈锅`,
		[]string{`汽水`, `引出管`},
	},
}

func TestParser_Parse(t *testing.T) {
	config.LoadSettings("/opt/test/config/test_case1.txt")
	parse := NewParser()
	for i := 0; i < len(parseCaseEntity); i++ {
		_, _ = parse.Parse(parseCaseEntity[i].Text)
		//if diff,ok := checkParsedResult(result,parseCaseEntity[i].Results);!ok{
		//	t.Errorf("结果未达到预期[i:%d,text:%s]\n",i,strings.Join(diff,"|"))
		//}
	}
}

func checkParsedResult(actual []config.Text, expected []string) ([]string, bool) {
	checker := make(map[string]bool)
	result := make([]string, 0)
	for i := 0; i < len(actual); i++ {
		key := string(actual[i])
		checker[key] = true
	}
	for i := 0; i < len(expected); i++ {
		_, ok := checker[expected[i]]
		if !ok {
			result = append(result, expected[i])
		}
	}
	if len(result) > 0 {
		return result, false
	}
	return result, true
}
