package main

import (
	"fmt"
	"time"

	"github.com/moonsphere/g2rtest/g2r"
)

func main() {
	fmt.Println("Invoking Rust from Go (blocking)...")
	start := time.Now()
	resp, err := g2r.Process(g2r.Request{
		Number:  21,
		Message: "hello from Go",
		Tags:    []string{"demo", "blocking"},
	})
	if err != nil {
		fmt.Printf("sync call failed: %v\n", err)
		return
	}
	fmt.Printf("sync response (%s): code=%d message=%q\n", time.Since(start), resp.Code, resp.Message)

	fmt.Println("Invoking Rust from Go (async)...")
	asyncCh, err := g2r.ProcessAsync(g2r.Request{
		Number:  37,
		Message: "async call",
		Tags:    []string{"demo", "async"},
	})
	if err != nil {
		fmt.Printf("async call setup failed: %v\n", err)
		return
	}

	fmt.Println("Doing other work while Rust future runs...")
	for i := 0; i < 3; i++ {
		fmt.Printf("tick %d\n", i+1)
		time.Sleep(80 * time.Millisecond)
	}

	asyncResp, ok := <-asyncCh
	if !ok {
		fmt.Println("async channel closed unexpectedly")
		return
	}
	fmt.Printf("async response: code=%d message=%q\n", asyncResp.Code, asyncResp.Message)
}
