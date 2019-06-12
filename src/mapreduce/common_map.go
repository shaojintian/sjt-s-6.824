package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
)

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue) {

	//	copyright  by  sjt@hnu.edu.cn

	// openfile
	inputFile , err := os.Open(inFile)

	if err != nil {
		log.Fatalln("doMap : open file failure " ,err)

	}
	//close file
	defer inputFile.Close()

	// read fileInfo
	fileInfo , err := inputFile.Stat()

	if err != nil{
		log.Fatalln("daMap : getState failure " ,err )

	}

	//create  byte slice  to  prepare  for  Reading  inputFile  and  Writing  inputFile to this byte slice
	data := make([]byte, fileInfo.Size() )
	//ignore  returned  first variable(n bytes which are  bytes of inputFile )
	//Read  inputFile and Write data of inputFile into data variable
	_,err = inputFile.Read(data)

	//read err
	if err != nil {
		log.Fatalln("doMap : Read inputFile failure ",err)

	}


	//recall  mapF

	KeyValues  := mapF(fileInfo.Name(), string(data))

	//fmt.Println("********",KeyValues)


	//create  nReduce  intermediate  files
	for i := 0 ; i<nReduce ; i++ {
		filename := reduceName(jobName,mapTask,i)
		//create a new empty  file  called  filename
		reduceFile , err := os.Create(filename)
		if err != nil {
			log.Fatalln("doMap : reduceFile create failure " ,err)
		}

		defer  reduceFile.Close()

		enc := json.NewEncoder(reduceFile)
		// ignore  index of  k-v array
		// pick right k-v
		for _ , kv  :=  range KeyValues {

			// pick right k-v for specific  reduceFile
			//%nReduce =  0 , 1 ,... ,nReduce -1
			if ihash(kv.Key)%(nReduce) == i {

				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalln("doMap: encode kv failure " ,err)
				}
			}

		}


	}











}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
