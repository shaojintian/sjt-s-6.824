






package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	//copyright by sjt@hnu.edu.cn
	//fmt.Printf("*******outFile : %s , nMap : %d *********" , outFile ,nMap)

	/*
		make map for (key , values )  to stores intermediate files

		A =1
		A =2
		A =3 ...
	*/

	kvMap := make(map[string]([]string ))

	/*
	read   files  like  mrtmp .test -x -x

	operate  read all k-v in kvMap
	 */

	for m:= 0; m <nMap; m++ {
		fileName := reduceName(jobName,m,reduceTask)

		inPutFile ,err := os.Open(fileName)

		if err != nil{
			log.Fatalln("doReduce  inPutfile  open err ",err)
		}

		defer inPutFile.Close()


		// read  KeyValue from inPutFile
		dec := json.NewDecoder(inPutFile)

		for dec.More() {

			var kv KeyValue
			err := dec.Decode(&kv)

			if err != nil {
				log.Fatalln("doReduce kv decode err ",err)

			}

			kvMap[kv.Key] = append(kvMap[kv.Key],kv.Value)

		}



	}



	/*
	sort   kvMap  by Key
	 */

	keysOrdered := make([]string , 0, len(kvMap))

	for key , _ :=  range kvMap {

		keysOrdered = append(keysOrdered , key)
	}

	sort.Strings(keysOrdered)



	/*
	encoder output  part
	 */


		outPutFile ,err := os.Create(outFile)

		if err != nil{
			log.Fatalln("doReduce  outPutFile generate err ",err)
		}

		enc := json.NewEncoder(outPutFile)

		for _,key:= range keysOrdered {

			err := enc.Encode(KeyValue{key, reduceF(key, kvMap[key])})

			if err != nil {
				log.Fatalln( "doReduce encoder err" ,err)
			}

		}

		defer outPutFile.Close()


}
