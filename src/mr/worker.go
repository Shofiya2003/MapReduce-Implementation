package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"json"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		filename, task, nReduce, taskNumber := RequestTask()
		if task == "map" {
			Map(mapf, nReduce, filename, taskNumber)
		} else if task == "reduce" {
			return
		} else {
			fmt.Println("No task available, retrying in 5 seconds...")
			time.Sleep(5 * time.Second) // Wait for 5 seconds before checking again
		}
	}

}

func Map(mapf func(string, string) []KeyValue, nReduce int, filename string, taskNumber int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))
	keyBuckets := make(map[string]int)
	for _, kv := range kva {
		key := kv.Key
		_, ok := keyBuckets[key]
		if !ok {
			reduceNumber := ihash(key) % nReduce
			keyBuckets[key] = reduceNumber
		}
	}

	i := 0
	for i < nReduce {
		intermediateFilename := "mr-" + string(taskNumber) + "_" + string(i)
		intermediateFile, err := os.Create(intermediateFilename)
		if err != nil {
			fmt.Println("Error creating file:", err)
			return
		}
		defer file.Close()
		enc := json.NewEncoder(intermediateFile)
		for _, kv := range kva {
			if i == keyBuckets[kv.Key] {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println("Error encoding key value pair:", err)
					return
				}
			}
		}
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func RequestTask() (string, string, int, int) {

	// declare an argument structure.
	args := TaskRequestArgs{}

	// declare a reply structure.
	reply := TaskRequestReply{}

	// send the RPC request, wait for the reply.
	call("Master.TaskRequest", &args, &reply)

	return reply.File, reply.Task, reply.NReduce, reply.TaskNumber

}

func NotifyTaskComplete(file string, task string, taskNumber int) {
	// declare an argument structure.
	args := TaskCompleteArgs{Timestamp: time.Now(), File: file, TaskNumber: taskNumber, Task: task}

	// declare a reply structure.
	reply := TaskRequestReply{}

	// send the RPC request, wait for the reply.
	call("Master.WorkerResponse", &args, &reply)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
