package main

import (
	"fmt"
	"strconv"
	"sync"
)

type  Demo struct {
	name string
	i int
}
func main(){

	ch := make(chan int,4)

	ch <- 1
	ch <- 2
	ch <- 3
	ch <- 4

	var wg sync.WaitGroup
	for i:=0 ; i<10;i++{


		var  demo  Demo
		demo.i =i
		demo.name = "sjt" + strconv.Itoa(i)
		//add 1 for wg
		wg.Add(1)

		go func(ii int ) {
			defer wg.Done()

			ch_fig := <- ch
			fmt.Printf("i:%d , ch_fig:%d demo.i:%d, demo.name:%s\n",ii,ch_fig,demo.i, demo.name)
			ch <- ch_fig

		}(i)


	}

	wg.Wait()

	close(ch)



}