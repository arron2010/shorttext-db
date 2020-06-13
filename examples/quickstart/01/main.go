package main

import (
	"context"
	"fmt"
	"time"
)

func test01() {
	timeout := 9 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout/3)
	cancel()
	select {
	case <-ctx.Done():
		fmt.Println("测试......")
	default:
		fmt.Println("测试 Default")
	}

}

func main() {

	test01()

}
