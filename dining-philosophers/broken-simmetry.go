package main

import (
	"fmt"
	"time"
)

const N = 5 // philosophers/forks
const R = 2 // iterations

var forks [N]chan struct{}

func think(i int) {
	fmt.Printf("Hmmm... Philosopher %d thinking...\n", i)
	time.Sleep(time.Second * 3)
}

func eat(i int) {
	fmt.Printf("Yummy! Philosopher %d eating...\n", i)
	time.Sleep(time.Second * 3)
}

func takeLeftForkFirst(i int) {
	<-forks[i]
	<-forks[(i+1)%N]
}

func takeRightForkFirst(i int) {
	<-forks[(i+1)%N]
	<-forks[i]
}

func releaseForks(i int) {
	forks[i] <- struct{}{}
	forks[(i+1)%N] <- struct{}{}
}

/*
For non-buffered channels, operations need to be synchronized:
at the same time a channel is requesting resources, one must be offering.
This makes sure every fork will be correctly taken and released by each philosopher
*/
func fork(i int) {
	for {
		forks[i] <- struct{}{}
		<-forks[i]
	}
}

func philosopher(i int) {
	think(i)

	if i % 2 == 0 {
		takeLeftForkFirst(i)
	} else {
		takeRightForkFirst(i)
	}

	eat(i)
	releaseForks(i)
}

func main() {
	for i := 0; i < N; i++ {
		forks[i] = make(chan struct{})
		go fork(i)
	}

	for i := 0; i < R; i++ {
		for j := 0; j < N; j++ {
			go philosopher(j)
		}
	}
	select {}
}
