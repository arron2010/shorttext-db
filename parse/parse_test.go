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
		text := cutter.CutForHybrid(strs[i])
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
