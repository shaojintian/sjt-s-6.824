package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//


// copyright by sjt@hnu.edu.cn
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// block  to wait all tasts  be  handled
	var wGroup  sync.WaitGroup

	// assign all tasks to workers
	for i:=0; i<ntasks; i++ {

		wGroup.Add(1)  //  monitor  one task
		// call() first arg
		// it is in go func()
		address := <- registerChan

		// call() second arg
		rpcname := "Worker.DoTask"
		//call() 3rd arg
		var doTaskArgs DoTaskArgs
		doTaskArgs.JobName = jobName
		doTaskArgs.File = mapFiles[i]
		doTaskArgs.Phase = phase
		doTaskArgs.TaskNumber = i
		doTaskArgs.NumOtherPhase =n_other

		//do call to works
		go func() {
			// when this goroutine  returns  , decrease the sync.WaitGroup
			defer wGroup.Done()

			//call worker RPC
			//这个worker是绝对会失败的，读到失败的worker就不把他返回
			for {
				if call(address, rpcname, doTaskArgs, nil)==false{
					fmt.Printf("ERROR:call worker%d  err!!!!!!!!\n",doTaskArgs.TaskNumber)
					address = <-registerChan
				}else {
					//还是会阻塞在最后一个worker，强制他退出。用子，主go程的约束关系！！！！！
					go func() {registerChan <- address}()
					break
				}

				//null
			}

			//concurrency to return   workers  who have finished his work !!!!!!
			//XXXXXXXX  registerChan <- address  XXXXXXXXXX
			//这里为什么要用并发？
			//这个是因为这个register channel没有缓冲，通道里只能有一个worker。
			//所以会阻塞在最后一个task之后，无法把最后一个任务的worker返回到channel里。
			//所以新建立一个子go程，虽然这个子go程还是会阻塞在最后一个，但是随着主go程的结束，子go程会强制退出
			//go func() {registerChan <- address}()
		}()

	}
	wGroup.Wait()  //  monitor all tasks have been  completed  to return


	fmt.Printf("Schedule: %v done\n", phase)
}
