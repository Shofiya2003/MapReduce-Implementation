package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task struct {
	task   string
	file   string
	status string
}
type Master struct {
	// Your definitions here.
	files                []string
	nReduce              int
	tasks                []Task
	unassignedPartitions []int
	completedMapTasks    []int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func remove(slice []int, idx int) []int {
	for i, _ := range slice {
		if i == idx {
			// Remove the element by slicing
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}

func (m *Master) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {

	// we can get started with reduce tasks
	if len(m.completedMapTasks) == len(m.files) {

	} else {
		mapTaskNumber := m.unassignedPartitions[0]
		m.unassignedPartitions = remove(m.unassignedPartitions, 0)
		reply.File = m.files[mapTaskNumber]
		reply.Task = "map"
		reply.TaskNumber = mapTaskNumber
		reply.Timestamp = time.Now()
		reply.NReduce = m.nReduce
	}

	return nil

}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{files: files, nReduce: nReduce}
	print("Master running...")
	// Your code here.

	m.server()
	return &m
}
