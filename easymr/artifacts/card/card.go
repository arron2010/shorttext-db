package card

import (
	"github.com/xp/shorttext-db/easymr/utils"
	"github.com/xp/shorttext-db/glogger"
	"strconv"
)

var logger = glogger.MustGetLogger("card")

func NewCard(ip string, port int32, alive bool, api string, seed bool) *Card {
	return &Card{
		IP:    ip,
		Port:  port,
		Alive: alive,
		API:   api,
		Seed:  seed,
	}
}

func (c *Card) IsInitialized() bool {
	if len(c.IP) > 0 {
		return true
	}
	return false
}

func (c *Card) GetFullIP() string {
	ipText := c.IP + ":" + strconv.Itoa(int(c.Port))

	return ipText
}

func (c *Card) GetFullExposureAddress() string {
	ipText := utils.MapToExposureAddress(c.IP) + ":" + strconv.Itoa(int(c.Port))
	return ipText
}

func (c *Card) GetFullExposureCard() Card {
	return Card{utils.MapToExposureAddress(c.IP), c.Port, c.Alive, c.API, c.Seed}
}

func (c *Card) GetFullEndPoint() string {
	return c.IP + ":" + strconv.Itoa(int(c.Port)) + "/" + c.API
}

func (c *Card) IsEqualTo(another *Card) bool {
	flag := (c.GetFullIP() == another.GetFullIP())
	//if (!flag){
	//	logger.Infof("is local server(%t), configed ip address is(%s,%s)",flag,c.GetFullIP(),another.GetFullIP() )
	//}

	//|| c.GetFullExposureAddress() == another.GetFullExposureAddress()

	//	address1 :=c.GetFullIP();
	//	address2 := another.GetFullIP();
	//	if address1 == "localhost:57852" && address2 =="192.168.37.150:57852" {
	//		return true
	//	};
	//	return c.GetFullIP() == another.GetFullIP()
	return flag
}

func (c *Card) IsSeed() bool {
	return c.Seed
}

func (c *Card) ToSeed() {
	c.Seed = true
}

func (c *Card) SetAlive(alive bool) {
	c.Alive = alive
}

// current RPC port
func Default() *Card {
	return &Card{utils.GetLocalIP(), utils.GetPort(), true, "", true}
}
