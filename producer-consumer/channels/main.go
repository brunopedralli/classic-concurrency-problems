package main

import (
	"fmt"
	"math/rand"
)

const P = 2 // number of producers
const C = 5 // number of consumers
const K = 3 // shared buffer size

func produce() int {
	num := rand.Intn(100)
	return num
}

func consume(num int) {
	fmt.Printf("[INFO] Consuming %d...\n", num)
}

func producer(ch chan<- int) {
	for {
		num := produce()
		fmt.Printf("[INFO] Produced %d!\n", num)
		ch <- num
	}
}

func consumer(ch <-chan int) {
	for {
		num := <-ch
		consume(num)
	}
}

func main() {
	ch := make(chan int, K)

	for i := 0; i < P; i++ {
		go producer(ch)
	}

	for i := 0; i < C; i++ {
		go consumer(ch)
	}

	select {}
}
