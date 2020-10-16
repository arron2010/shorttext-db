package main

import (
	"context"
	"fmt"
	"github.com/xp/shorttext-db/network"
	"google.golang.org/grpc"
)

func main() {
	sendPut()
}
func sendPut() {
	addr := "127.0.0.1:5009"
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		fmt.Println(err)
		return
	}
	client := network.NewStreamProxyClient(conn)
	batchMessage := &network.BatchMessage{}
	client.Send(context.Background(), batchMessage)
}
