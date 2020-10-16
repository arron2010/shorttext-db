package main

import (
	"context"
	"fmt"
	"github.com/xp/shorttext-db/network"
	//"github.com/xp/shorttext-db/utils"
	"google.golang.org/grpc"
	"math/rand"
	"time"
)

func test01() {

	var result *network.BatchMessage
	conn, err := grpc.Dial("127.0.0.1:5009", grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
	}
	defer conn.Close()
	c := network.NewStreamProxyClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10000)
	defer cancel()
	msg := &network.BatchMessage{}
	msg.Term = uint64(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(1000000))
	msg.Messages = make([]*network.Message, 0)
	count := uint32(3)
	msg.Messages = append(msg.Messages, &network.Message{Term: msg.Term, From: 1, To: 2, Text: "测试2", Count: count})
	msg.Messages = append(msg.Messages, &network.Message{Term: msg.Term, From: 1, To: 3, Text: "测试3", Count: count})
	msg.Messages = append(msg.Messages, &network.Message{Term: msg.Term, From: 1, To: 4, Text: "测试4", Count: count})

	result, err = c.Send(ctx, msg)
	if err != nil {
		fmt.Println(err)
	}
	if result == nil {
		panic("结果为空")
		return
	}
	for i := 0; i < len(result.Messages); i++ {
		fmt.Printf("Term:%d From:%d To:%d Text:%s\n",
			result.Messages[i].Term, result.Messages[i].From, result.Messages[i].To, result.Messages[i].Text)
	}

}
func main() {
	for i := 0; i < 1; i++ {
		test01()
		time.Sleep(1 * time.Second)
	}
	//utils.WaitFor()
}
