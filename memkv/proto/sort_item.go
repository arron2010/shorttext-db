package proto

import "bytes"

func (d *DbItems) Len() int { return len(d.Items) }

func (d *DbItems) Swap(i, j int) { d.Items[i], d.Items[j] = d.Items[j], d.Items[i] }
func (d *DbItems) Less(i, j int) bool {
	result := bytes.Compare(d.Items[i].Key, d.Items[j].Key)
	if result < 0 {
		return true
	} else {
		return false
	}
}
