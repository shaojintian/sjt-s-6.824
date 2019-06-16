package main

import (
	"math/rand"
	"time"
)
func init(){
	//以时间作为初始化种子
	rand.Seed(time.Now().UnixNano())
}
func main() {

	timer := time.NewTimer(1*time.Second)
	<-timer.C
	timer.Reset(time.Second)
	timer.Reset(time.Second)
	<-timer.C
	//<-timer.C

}