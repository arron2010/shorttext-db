package collaborator

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xp/shorttext-db/easymr/artifacts/card"
	"github.com/xp/shorttext-db/easymr/artifacts/digest"
	"github.com/xp/shorttext-db/easymr/artifacts/iremote"
	"github.com/xp/shorttext-db/easymr/artifacts/message"
	"github.com/xp/shorttext-db/easymr/cmd"
	"github.com/xp/shorttext-db/easymr/constants"
	"github.com/xp/shorttext-db/easymr/helpers/messageHelper"
	"github.com/xp/shorttext-db/glogger"
	"io/ioutil"
	"os"
	"sync"
	"time"
)

var mu = sync.Mutex{}
var traceLogger = glogger.MustGetLogger("trace")

// A central collection of Collaborators across the cluster.
type Case struct {
	CaseID string `json:"caseid,omitempty"`
	*Exposed
	*Reserved
	CardList []*card.Card `json:"-"`
}

// The exposed card addresses.
type Exposed struct {
	Cards     map[string]*card.Card `json:"cards,omitempty"`
	TimeStamp int64                 `json:"timestamp,omitempty"`
}

// The reserved card addresses.
type Reserved struct {
	// local is the Card of localhost
	Local       card.Card `json:"local,omitempty"`
	Coordinator card.Card `json:"coordinator,omitempty"`
}

func (c *Case) readStream() error {
	//xiaopeng modified on 2018-12-30
	//case配置从命令行的系统变量读取
	var config *cmd.SysVars = cmd.Vars()
	logger.Infof("case文件地址：%s\n", config.CasePath)
	bytes, err := ioutil.ReadFile(config.CasePath)
	if err != nil {
		panic(err)
	}
	// unmarshal, overwrite default if already existed in config file
	if err := json.Unmarshal(bytes, &c); err != nil {
		logger.Error(err.Error())
		return err
	}
	c.CardList = make([]*card.Card, len(c.Cards))
	var index int = 0
	for _, value := range c.Cards {
		c.CardList[index] = value
		index++
	}
	return nil
}

func (c *Case) writeStream() error {
	mu.Lock()
	defer mu.Unlock()
	mal, err := json.Marshal(&c)
	err = ioutil.WriteFile(constants.DEFAULT_CASE_PATH, mal, os.ModeExclusive)
	return err
}

func (c *Case) Stamp() *Case {
	c.TimeStamp = time.Now().Unix()
	return c
}

func (c *Case) GetCluster() string {
	return c.CaseID
}

func (c *Case) GetDigest() iremote.IDigest {
	return &digest.Digest{c.Cards, c.TimeStamp}
}

func (c *Case) Update(dgst iremote.IDigest) {
	c.Cards = dgst.GetCards()
	c.TimeStamp = dgst.GetTimeStamp()
}

func (c *Case) Terminate(key string) *Case {
	mu.Lock()
	defer mu.Unlock()
	delete(c.Cards, key)
	return c
}

func (c *Case) returnRandomByPos(pos int) *card.Card {
	if l := len(c.Cards); pos > l {
		pos = pos % l
	}
	counter := 0
	for _, a := range c.Cards {
		if counter == pos {
			return a
		}
		counter++
	}
	return &card.Card{}
}

func (c *Case) ReturnByPos(pos int) *card.Card {
	//	traceLogger.Infof("消息分发----------主机:%s  附属机索引:%d",c.Local.GetFullIP(),pos)
	//mu.Lock()
	//defer mu.Unlock()
	//fmt.Println("-----------------------------------------------",pos)
	return c.CardList[pos]
}
func (c *Case) GetRemoteIP() []*card.Card {
	remoteIPs := make([]*card.Card, 0)
	if len(c.CardList) == 0 {
		return remoteIPs
	}
	for _, item := range c.CardList {
		if item.IsEqualTo(&c.Local) {
			continue
		}
		remoteIPs = append(remoteIPs, item)
	}
	return remoteIPs
}

func (c *Case) GetAllIP() []*card.Card {
	return c.CardList
}

func (c *Case) HandleMessage(in *message.CardMessage) (*message.CardMessage, error) {
	// return if message is wrongly sent
	var (
		out *message.CardMessage = message.NewCardMessage()
		err error                = nil
	)

	if err = c.Validate(in, out); err != nil {
		return out, err
	}
	var (
		// local digest
		ldgst = c.GetDigest()
		// remote digest
		rdgst = in.GetDigest()
		// feedback digest
		fbdgst = ldgst
	)
	switch in.GetType() {
	case message.CardMessage_SYNC:
		// msg has a more recent timestamp
		if messageHelper.Compare(ldgst, rdgst) {
			fbdgst = messageHelper.Merge(ldgst, rdgst)
			// update digest to local
			c.Update(fbdgst)
		}
		// update digest to feedback
		out.Update(fbdgst)

		// return ack message
		out.SetType(message.CardMessage_ACK)
		out.SetStatus(constants.GOSSIP_HEADER_OK)
	case message.CardMessage_ACK:
		// msg has a more recent timestamp
		if messageHelper.Compare(ldgst, rdgst) {
			fbdgst = messageHelper.Merge(ldgst, rdgst)
			// update digest to local
			c.Update(fbdgst)
		}
		// return ack message
		out.SetType(message.CardMessage_ACK2)
		out.SetStatus(constants.GOSSIP_HEADER_OK)
	case message.CardMessage_ACK2:
		// return ack message
		out.SetType(message.CardMessage_ACK3)
		out.SetStatus(constants.GOSSIP_HEADER_OK)
	case message.CardMessage_ACK3:
	default:
		out.SetStatus(constants.GOSSIP_HEADER_UNKNOWN_MSG_TYPE)
		err = constants.ERR_UNKNOWN_MSG_TYPE
	}
	out.SetTo(in.GetFrom())
	out.SetFrom(in.GetTo())
	out.SetCluster(c.GetCluster())
	return out, nil
}

func (c *Case) Validate(in *message.CardMessage, out *message.CardMessage) error {

	if c.GetCluster() != in.GetCluster() {
		out.SetStatus(constants.GOSSIP_HEADER_CASE_MISMATCH)
		msg := fmt.Sprintf("goplatform: case mismatch error, source is %s , input is %s ", c.GetCluster(), in.GetCluster())
		err := errors.New(msg)
		return err
	}
	if to := in.GetTo(); !c.Local.IsEqualTo(to) {
		logger.Error(c)
		out.SetStatus(constants.GOSSIP_HEADER_COLLABORATOR_MISMATCH)
		return constants.ERR_COLLABORATOR_MISMATCH
	}
	return nil
}
