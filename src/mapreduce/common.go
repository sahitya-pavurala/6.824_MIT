package mapreduce

import (
	"fmt"
	"strconv"
	"io"
	"log"
)

var (
	infoLog  *log.Logger
	errorLog  *log.Logger
)

// Debugging enabled?
const debugEnabled = false

// debug() will only print if debugEnabled is true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

func initLogger(infoHandle io.Writer, errorHandle io.Writer) (*log.Logger, *log.Logger){
	infoLog = log.New(infoHandle,
		"INFO: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	errorLog = log.New(errorHandle,
		"ERROR: ",
		log.Ldate|log.Ltime|log.Lshortfile)

	return infoLog, errorLog
}

func checkError(e error) {
	if e != nil {
		ERROR.Println(e.Error())
		return
	}
}

type sortByKey []KeyValue
func (kvRecords sortByKey) Less(i ,j int) bool{
	return kvRecords[i].Key < kvRecords[j].Key
}