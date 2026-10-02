package main

import (
	"fmt"
	"time"
	"sync"
)

const N = 5
const R = 2

func test(id int) {

}


func print(wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("Hello, World!")
}

func main() {
	var wg sync.WaitGroup
	ch := make(chan int, 2)
	ch <- 2
	<-ch
	wg.Add(1)
	go print(&wg)
	time.Sleep(time.Second)
	wg.Wait()
}
