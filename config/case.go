package config

import "strconv"

type Card struct {
	ID    uint64
	Name  string
	IP    string
	Port  uint32
	Alive bool
}

type Case struct {
	CaseId     string
	MasterCard *Card
	//不包含主节点
	CardList []*Card
	Local    *Card
}

func (c *Case) GetMaster() *Card {
	return c.MasterCard
}

func (c *Case) GetUrls() []string {
	url := "http://" + c.MasterCard.IP + ":" + strconv.Itoa(int(c.MasterCard.Port))
	urls := make([]string, 0, 0)
	urls = append(urls, url)
	for i := 0; i < len(c.CardList); i++ {
		url = "http://" + c.CardList[i].IP + ":" + strconv.Itoa(int(c.CardList[i].Port))
		urls = append(urls, url)
	}
	return urls
}

func (c *Case) GetCardList() []*Card {
	return c.CardList
}

//func (c *Case)GetChildCardNames()[]string{
//	return c.childCardNames
//}
//
