package parse

import (
	"bytes"
	"github.com/xp/shorttext-db/config"
	"github.com/yanyiwu/gojieba"
	"regexp"
	"strings"
	"unicode/utf8"
)

/*
100 缺省值
101  全部为数字
102  单个字符占比超过70%
*/
const (
	CUTTER_DEFAULT     = 100
	CUTTER_NUMBER      = 101
	CUTTER_SINGLE_CHAR = 102
)
const defaultMaxLen = 4

/*
中文分词
*/
type Cutter interface {
	HMMCut(text string) []string
	AddWord(word string)
	CutForChinese(text string, maxLen int) []string
	CutForASCII(text string, maxLen int) []string
	CutForHybrid(text string, maxLen int) []string
}

func NewCutter(config *config.Config) *JiebaCutter {
	instance := &JiebaCutter{}
	instance.initialize(config)
	return instance
}

type JiebaCutter struct {
	impl             *gojieba.Jieba
	regReplaced      *regexp.Regexp
	regScopeReplaced *regexp.Regexp
	regASCII         *regexp.Regexp
	regSeparator     *regexp.Regexp
	regNum           *regexp.Regexp
}

func (j *JiebaCutter) initialize(config *config.Config) {

	gojieba.DICT_PATH = config.DictPath
	gojieba.HMM_PATH = config.HmmPath
	gojieba.USER_DICT_PATH = config.UserDictPath
	gojieba.IDF_PATH = config.IdfPath
	gojieba.STOP_WORDS_PATH = config.StopWordsPath

	//删除非中文、字符、数字（除去点和百分号)
	//\.\%°
	j.regReplaced = regexp.MustCompile(`[^\p{L}\p{N}\p{Han}]`)
	//匹配非中文字符
	j.regASCII = regexp.MustCompile(`^[^\p{Han}]+$`)

	//删除指定字符
	j.regScopeReplaced = regexp.MustCompile(`[φ,δ,Φ]`)

	j.regSeparator = regexp.MustCompile("(\\+|\\-|\\*|\\/|\\=|\\<|\\>|≥|≤|×)")

	//数字
	j.regNum = regexp.MustCompile(`^\d+(\.\d+)?[%]?$`)
	j.impl = gojieba.NewJieba()

}
func (j *JiebaCutter) HMMCut(text string) []string {
	result := j.impl.Cut(text, true)
	return result
}

/*
先对混合字符进行分词，再判断词是否为非中文，如果为非中文，就与下一个词进行合并。
*/
func (j *JiebaCutter) CutForHybrid(text string, maxLen int) []string {
	if maxLen == 0 {
		maxLen = defaultMaxLen
	}

	cuttText := j.clean(strings.ToUpper(text))

	//长度小于４个字符，直接返回
	if utf8.RuneCountInString(cuttText) <= maxLen {
		return []string{cuttText}
	}

	newItems := make([]string, 0, 8)
	result := j.impl.Cut(cuttText, true)
	l := len(result)

	if l < 2 {
		newItems = append(newItems, j.truncateStr(cuttText, maxLen))
		return newItems
	}

	var temp string = result[0]
	for i := 1; i < l; i++ {
		if j.isASCII(temp) {
			temp = temp + result[i]
		} else {
			newItems = append(newItems, j.truncateStr(temp, maxLen))
			temp = result[i]
		}
	}
	//由于最后一个词合并后，不会append到数组，所以再进行一次append
	newItems = append(newItems, j.truncateStr(temp, maxLen))
	return newItems
}

func (j *JiebaCutter) CutForChinese(text string, maxLen int) []string {
	if maxLen == 0 {
		maxLen = defaultMaxLen
	}
	//长度小于４个字符，直接返回
	if utf8.RuneCountInString(text) <= maxLen {
		return []string{text}
	}
	result := j.impl.CutForSearch(text, true)

	return result
}

func (j *JiebaCutter) CutForASCII(text string, maxLen int) []string {

	if maxLen == 0 {
		maxLen = defaultMaxLen
	}

	upperText := strings.ToUpper(text)
	ascText := j.cleanForASCII(upperText)
	//长度小于6个字符，直接返回
	if utf8.RuneCountInString(ascText) <= maxLen {
		return []string{ascText}
	}
	result := make([]string, 0, 4)

	//cleanedText :=j.clean(ascText)
	//result = append(result,cleanedText)

	words := j.regSeparator.Split(ascText, -1)
	if len(words) == 1 {
		return result
	}
	format := j.checkFormat(words)
	switch format {
	case CUTTER_NUMBER:
		return result
	case CUTTER_SINGLE_CHAR:
		newWords := j.combine(words)
		for _, w := range newWords {
			result = append(result, j.truncateStr(w, maxLen))
		}
	default:
		for _, w := range words {
			result = append(result, j.truncateStr(w, maxLen))
		}
	}
	return result
}

func (j *JiebaCutter) checkFormat(words []string) int {
	bNum := j.isNum(words)
	if bNum {
		return CUTTER_NUMBER
	}
	count := j.countSingleChar(words)
	if float32(count)/float32(len(words)) >= 0.7 {
		return CUTTER_SINGLE_CHAR
	}
	return CUTTER_DEFAULT
}

func (j *JiebaCutter) countSingleChar(words []string) int {
	count := 0
	for _, w := range words {
		if utf8.RuneCountInString(w) == 1 {
			count = count + 1
		}
	}
	return count
}
func (j *JiebaCutter) isNum(words []string) bool {

	for _, w := range words {
		if !j.regNum.MatchString(w) {
			return false
		}
	}
	return true
}
func (j *JiebaCutter) combine(words []string) []string {
	var max string
	max = ""
	var index int
	var l int
	for i, w := range words {
		l1 := utf8.RuneCountInString(w)
		l2 := utf8.RuneCountInString(max)
		if l1 > l2 {
			max = w
			index = i
			l = l1
		}
	}
	if l != 1 {
		words[index] = ""
	}
	newWords := combineStr(words)
	newWords = append(newWords, max)
	return newWords
}

func (j *JiebaCutter) AddWord(word string) {
	//	j.impl.AddWord(word)
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

func (j *JiebaCutter) cleanForASCII(text string) string {
	result := j.regScopeReplaced.ReplaceAllString(text, "")
	return result
}

func (j *JiebaCutter) truncateStr(text string, maxLen int) string {
	if len(text) > maxLen {
		return text[:maxLen]
	}
	return text
}
func combineStr(strList []string) []string {
	newList := make([]string, 0)
	strLen := len(strList)
	ended := false
	for i := 0; i < strLen-1; i++ {
		if utf8.RuneCountInString(strList[i]) == 1 {
			newList = append(newList, strings.Join([]string{strList[i], strList[i+1]}, ""))
			i++
			if i == strLen-1 {
				ended = true
			}
		} else {
			newList = append(newList, strList[i])
		}
	}
	if !ended {
		if utf8.RuneCountInString(strList[strLen-1]) == 1 && len(newList) > 0 {
			endStr := newList[len(newList)-1]
			newList[len(newList)-1] = endStr + strList[strLen-1]
		} else {
			newList = append(newList, strList[strLen-1])
		}
	}
	return newList
}

func findAndSubString(reg *regexp.Regexp, text string) ([]string, string) {
	if !reg.MatchString(text) {
		return []string{}, text
	}
	positions := reg.FindAllStringIndex(text, -1)
	if len(positions) == 0 {
		return []string{}, ""
	}
	list, newPos := getStringArray(text, positions)
	subStr := subText(text, newPos)
	return list, subStr
}

func getStringArray(text string, positions [][]int) ([]string, [][]int) {
	found := make([]string, 0)
	//newPos := make([][]int,0)
	posLen := len(positions)
	for i := 0; i < posLen; i++ {
		strItem := string(text[positions[i][0]:positions[i][1]])
		found = append(found, strItem)
	}
	return found, positions
}

func checkPosition(i int, pos [][]int) bool {
	for j := 0; j < len(pos); j++ {
		if i >= pos[j][0] && i < pos[j][1] {
			return false
		}
	}
	return true
}

func subText(text string, positions [][]int) string {
	var remain bytes.Buffer = bytes.Buffer{}
	length := len(text)
	for i := 0; i < length; i++ {
		if !checkPosition(i, positions) {
			continue
		}
		remain.WriteByte(text[i])
	}
	str := remain.String()
	return str
}
