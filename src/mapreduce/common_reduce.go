package mapreduce

import (
	"os"
	"encoding/json"
	"io"
	"sort"
	"strings"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//


	//intermediateFileName := reduceName(jobName, m)

	INFO.Printf("Starting reduce task %d", reduceTask)

	recordsForReduceTask := make([]KeyValue, 0)
	for mapTaskNum := 0; mapTaskNum < nMap; mapTaskNum++{
		fName := reduceName(jobName, mapTaskNum, reduceTask)
		reduceFile, err := os.Open(fName)
		checkError(err)
		defer reduceFile.Close()

		jsonDecoder := json.NewDecoder(reduceFile)

		for {
				var kv KeyValue
				if err := jsonDecoder.Decode(&kv); err == io.EOF {
					break
					}
				checkError(err)
				recordsForReduceTask = append(recordsForReduceTask, kv)
				}

			}
	INFO.Println("Implementing sort on the records")
	//Sort and combine can be replaced with a map
	sort.Slice(recordsForReduceTask, func(i, j int) bool {
		switch strings.Compare(recordsForReduceTask[i].Key, recordsForReduceTask[j].Key) {
		case -1:
			return true
		}
		return false
	})

	INFO.Printf("Number of records for reduce task %d is %d", reduceTask, len(recordsForReduceTask))

	var currentKey string
	var valuesPerKey []string
	//valuesForEachKey := make(map[string][]string)
	outputFile, err := os.Create(outFile)
	checkError(err)
	defer outputFile.Close()

	jsonEncoder := json.NewEncoder(outputFile)

	for _,record := range recordsForReduceTask{
		reduceId := record.Key
		val := record.Value

		if currentKey != reduceId{
			if valuesPerKey != nil{
				//INFO.Printf("Number of values for key %s is %d", currentKey, len(valuesPerKey))
				checkError(jsonEncoder.Encode(KeyValue{currentKey, reduceF(currentKey, valuesPerKey)}))
			}
			valuesPerKey = make([]string, 0)
			currentKey = reduceId
		}

		valuesPerKey = append(valuesPerKey, val)

	}

	if valuesPerKey != nil{
		//INFO.Printf("Number of values for key %s is %d", currentKey, len(valuesPerKey))
		checkError(jsonEncoder.Encode(KeyValue{currentKey, reduceF(currentKey, valuesPerKey)}))
	}

}
