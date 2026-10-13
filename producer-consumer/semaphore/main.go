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

func producer(notFull <-chan struct{}, notEmpty chan<- struct{}, buffer chan<- int) {
	for {
		num := produce()
		fmt.Printf("[INFO] Produced %d!\n", num)
		<-notFull
		buffer <- num
		notEmpty <- struct{}{}
	}
}

func consumer(notFull chan<- struct{}, notEmpty <-chan struct{}, buffer <-chan int) {
	for {
		<-notEmpty
		num := <-buffer
		notFull <- struct{}{}
		consume(num)
	}
}

func main() {
	buffer := make(chan int, K)        // finite queue
	notEmpty := make(chan struct{}, C) // initialized with 0

	notFull := make(chan struct{}, P) // initialized with P
	for i := 0; i < P; i++ {
		notFull <- struct{}{}
	}

	go producer(notFull, notEmpty, buffer)
	go consumer(notFull, notEmpty, buffer)

	select {}
}
