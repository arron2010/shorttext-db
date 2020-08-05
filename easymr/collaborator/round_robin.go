package collaborator

import "github.com/xp/shorttext-db/easymr/artifacts/card"

type Balancer struct {
	items   []*card.Card
	weights map[int]int
}

func NewBalancer() {

}
func (b *Balancer) Pick() *card.Card {
	index, value := b.getMinWeight()
	b.weights[index] = value + 1
	return b.items[index]
}

func (b *Balancer) getMinWeight() (int, int) {
	var minValue int = 0
	var minIndex = 0
	for k, v := range b.weights {
		if v < minValue {
			minValue = v
			minIndex = k
		}
	}
	return minIndex, minValue
}
