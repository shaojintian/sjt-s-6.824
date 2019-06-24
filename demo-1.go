package main

import (
	"fmt"
	"math/rand"
	"time"
)
func init(){
	//以时间作为初始化种子
	rand.Seed(time.Now().UnixNano())
}
func main() {

	const(
		b =  10 * (iota)
		kb
		mb
		gb
		tb
		pb
	)
	s := []int{1,2,3}
	fmt.Println(s[0:2])
}