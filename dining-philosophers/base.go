package main

import (
	"fmt"
	"time"
)

const N = 5 // philosophers/forks
const R = 2 // iterations

var forks [N]chan bool

func think(i int) {
	fmt.Printf("Hmmm... Philosopher %d thinking...\n", i)
	time.Sleep(time.Second * 3)
}

func eat(i int) {
	fmt.Printf("Yummy! Philosopher %d eating...\n", i)
	time.Sleep(time.Second * 3)
}

func takeForks(i int) {
	<-forks[i]
	<-forks[(i+1)%N]
}

func releaseForks(i int) {
	forks[i] <- true
	forks[(i+1)%N] <- true
}

/*
For non-buffered channels, operations need to be synchronized:
at the same time a channel is requesting resources, one must be offering.
This makes sure every fork will be correctly taken and released by each philosopher
*/
func fork(i int) {
	for {
		forks[i] <- true
		<-forks[i]
	}
}

func philosopher(i int) {
	think(i)
	takeForks(i)
	eat(i)
	releaseForks(i)
}

func main() {
	for i := 0; i < N; i++ {
		forks[i] = make(chan bool)
		go fork(i)
	}

	for i := 0; i < R; i++ {
		for j := 0; j < N; j++ {
			go philosopher(j)
		}
	}
	select {}
}
