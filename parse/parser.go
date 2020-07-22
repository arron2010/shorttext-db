package parse

import (
	"github.com/xp/shorttext-db/config"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	CHINESE = 1
	ASCII   = 2
	HYBRID  = 3
	EMPTY   = 0
)

type textItem struct {
	Text     string
	ItemType int
	Index    int
}
type IParse interface {
	Parse(text string) ([]config.Text, error)
}

type Parser struct {
	cutter          Cutter
	regCompletedHan *regexp.Regexp
	regPartitionHan *regexp.Regexp
	regSeparator    *regexp.Regexp
}

func NewParser() IParse {
	cfg := config.GetConfig()
	p := &Parser{}
	p.cutter = NewCutter(cfg)
	//字符从头到尾都是中文字符
	p.regCompletedHan = regexp.MustCompile(`^\p{Han}+$`)
	//部分字符是中文
	p.regPartitionHan = regexp.MustCompile(`\p{Han}+`)
	//基于, \ ; 空格等字符进行分割
	p.regSeparator = regexp.MustCompile("(,|\\||;|\\s|　)")
	return p
}

func (p *Parser) Parse(text string) ([]config.Text, error) {
	textItems := p.split(text)
	result := make([]config.Text, 0, 0)
	var words []string
	var item config.Text
	checker := make(map[string]bool, 8)
	for _, val := range textItems {
		switch val.ItemType {
		case CHINESE:
			words = p.cutter.CutForChinese(val.Text)
		case ASCII:
			words = p.cutter.CutForASCII(val.Text)
		case HYBRID:
			words = p.cutter.CutForHybrid(val.Text)
		}
		for _, w := range words {
			if len(w) == 0 {
				continue
			}
			if utf8.RuneCountInString(w) == 1 {
				continue
			}
			_, ok := checker[w]
			if ok {
				continue
			}
			item = config.Text(w)
			result = append(result, item)
			checker[w] = true
		}
	}
	return result, nil
}

func (p *Parser) split(text string) []textItem {
	segmentations := strings.Split(text, config.PARSING_RECORD_SEP)
	result := make([]textItem, 0)
	for i, seg := range segmentations {
		childSegs := p.regSeparator.Split(seg, -1)
		for _, val := range childSegs {
			childrenItems := p.splitChild(i, val)
			result = append(result, childrenItems...)
		}
	}
	return result
}

func (p *Parser) splitChild(index int, text string) []textItem {
	result := make([]textItem, 0)
	var item textItem
	if p.regCompletedHan.MatchString(text) {
		item = textItem{Text: text, ItemType: CHINESE, Index: index}
		result = append(result, item)
		return result
	} else {
		if p.regPartitionHan.MatchString(text) {
			item = textItem{Text: text, ItemType: HYBRID, Index: index}
			result = append(result, item)
			return result
		} else {
			item = textItem{Text: text, ItemType: ASCII, Index: index}
			result = append(result, item)
			return result
		}
	}
}
