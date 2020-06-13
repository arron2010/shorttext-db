package filedb

import (
	"testing"
)

func TestNewSequence(t *testing.T) {
	s := NewSequence(0)
	s.SetStart(0)
	//for i:=0;i< 100;i++{
	//	fmt.Println(s.Next())
	//}
	s.Close()
}
