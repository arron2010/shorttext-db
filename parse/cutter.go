package parse

import (
	"github.com/xp/shorttext-db/config"
	"github.com/yanyiwu/gojieba"
	"regexp"
	"unicode/utf8"
)

/*
中文分词
*/
type Cutter interface {
	HMMCut(text string) []string
	AddWord(word string)
	CutForChinese(text string) []string
	CutForASCII(text string) []string
	CutForHybrid(text string) []string
}

func NewCutter(config *config.Config) *JiebaCutter {
	instance := &JiebaCutter{}
	instance.initialize(config)
	return instance
}

type JiebaCutter struct {
	impl        *gojieba.Jieba
	regReplaced *regexp.Regexp
	regASCII    *regexp.Regexp
}

func (j *JiebaCutter) HMMCut(text string) []string {
	result := j.impl.Cut(text, true)
	return result
}

func (j *JiebaCutter) CutForASCII(text string) []string {
	result := j.impl.CutForSearch(text, true)
	return result
}

/*
先对混合字符进行分词，再判断词是否为非中文，如果为非中文，就与下一个词进行合并。
*/
func (j *JiebaCutter) CutForHybrid(text string) []string {
	cuttText := j.clean(text)
	newItems := make([]string, 0, 8)
	//长度小于４个字符，直接返回
	if utf8.RuneCountInString(text) < 5 {
		newItems = append(newItems, cuttText)
		return newItems
	}

	result := j.impl.Cut(cuttText, true)
	l := len(result)

	if l < 2 {
		newItems = append(newItems, cuttText)
		return newItems
	}

	var temp string = result[0]
	for i := 1; i < l; i++ {
		if j.isASCII(temp) {
			temp = temp + result[i]
		} else {
			newItems = append(newItems, temp)
			temp = result[i]
		}
	}
	//由于最后一个词合并后，不会append到数组，所以再进行一次append
	newItems = append(newItems, temp)
	return newItems
}

func (j *JiebaCutter) CutForChinese(text string) []string {

	result := j.impl.CutForSearch(text, true)
	return result
}

func (j *JiebaCutter) AddWord(word string) {
	//	j.impl.AddWord(word)
}

func (j *JiebaCutter) initialize(config *config.Config) {

	gojieba.DICT_PATH = config.DictPath
	gojieba.HMM_PATH = config.HmmPath
	gojieba.USER_DICT_PATH = config.UserDictPath
	gojieba.IDF_PATH = config.IdfPath
	gojieba.STOP_WORDS_PATH = config.StopWordsPath

	//删除非中文、字符、数字（除去点和百分号)
	j.regReplaced = regexp.MustCompile(`[^\p{L}\p{N}\p{Han}\.\%°]`)
	//匹配非中文字符
	j.regASCII = regexp.MustCompile(`^[^\p{Han}]+$`)
	j.impl = gojieba.NewJieba()

}

func (j *JiebaCutter) isASCII(text string) bool {
	if j.regASCII.MatchString(text) && utf8.RuneCountInString(text) < 4 {
		return true
	}
	return false
}

func (j *JiebaCutter) clean(text string) string {
	result := j.regReplaced.ReplaceAllString(text, "")
	return result
}

//
//func (p *Parser) findAndSubString(reg *regexp.Regexp, text string) ([]string, string) {
//	if !reg.MatchString(text) {
//		return []string{}, text
//	}
//	positions := reg.FindAllStringIndex(text, -1)
//	if len(positions) == 0 {
//		return []string{}, ""
//	}
//	list, newPos := p.getStringArray(text, positions)
//	subStr := p.subText(text, newPos)
//	return list, subStr
//}
//
//
//func (p *Parser) getStringArray(text string, positions [][]int) ([]string, [][]int) {
//	found := make([]string, 0)
//	//newPos := make([][]int,0)
//	posLen := len(positions)
//	for i := 0; i < posLen; i++ {
//		strItem := string(text[positions[i][0]:positions[i][1]])
//		found = append(found, strItem)
//	}
//	return found, positions
//}
//
//func (p *Parser) checkPosition(i int, pos [][]int) bool {
//	for j := 0; j < len(pos); j++ {
//		if i >= pos[j][0] && i < pos[j][1] {
//			return false
//		}
//	}
//	return true
//}
//
//func (p *Parser) subText(text string, positions [][]int) string {
//	var remain bytes.Buffer = bytes.Buffer{}
//	length := len(text)
//	for i := 0; i < length; i++ {
//		if !p.checkPosition(i, positions) {
//			continue
//		}
//		remain.WriteByte(text[i])
//	}
//	str := remain.String()
//	return str
//}
