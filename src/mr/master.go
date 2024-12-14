package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Master struct {
	// Your definitions here.
	files                   []string
	nReduce                 int
	Tasks                   map[string]time.Time
	unassignedPartitions    []int
	completedMapTasks       []int
	unassignmedReducedTasks []int
	completedReduceTasks    []int
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
	if len(m.unassignedPartitions) != 0 {
		mapTaskNumber := m.unassignedPartitions[0]
		// print(mapTaskNumber)
		m.unassignedPartitions = remove(m.unassignedPartitions, 0)
		reply.File = m.files[mapTaskNumber]
		reply.Task = "map"
		reply.TaskNumber = mapTaskNumber
		reply.Timestamp = time.Now()
		reply.NReduce = m.nReduce
		reply.MTasks = len(m.files)
		taskName := strconv.Itoa(mapTaskNumber) + "_map"
		m.Tasks[taskName] = time.Now()
	} else if len(m.completedMapTasks) == len(m.files) && len(m.unassignmedReducedTasks) != 0 {
		reduceTaskNumber := m.unassignmedReducedTasks[0]
		m.unassignmedReducedTasks = remove(m.unassignmedReducedTasks, 0)
		reply.Task = "reduce"

		reply.TaskNumber = reduceTaskNumber
		reply.NReduce = m.nReduce
		reply.MTasks = len(m.files)
		taskName := strconv.Itoa(reduceTaskNumber) + "_reduce"
		m.Tasks[taskName] = time.Now()
		// println("reduce task assigned for %s ", reduceFile)
	}

	return nil

}

func (m *Master) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	taskNumber := args.TaskNumber
	task := args.Task
	// println("Task completed %s", taskNumber)
	if task == "map" {
		m.completedMapTasks = append(m.completedMapTasks, taskNumber)
	} else if task == "reduce" {
		m.completedReduceTasks = append(m.completedReduceTasks, taskNumber)
	}
	return nil

}

func (m *Master) TaskFail(args *TaskFailArgs, reply *TaskFailReply) {
	taskNumber := args.TaskNumber
	task := args.Task
	// println("Task completed %s", taskNumber)
	if task == "map" {
		m.unassignedPartitions = append(m.unassignedPartitions, taskNumber)
	} else if task == "reduce" {
		m.unassignmedReducedTasks = append(m.unassignmedReducedTasks, taskNumber)
	}
}

func (m *Master) CheckWorkerStatus() {
	ticker := time.NewTicker(9 * time.Second) // Run every 10 seconds
	defer ticker.Stop()

	for range ticker.C {
		for taskName, lastActive := range m.Tasks {
			taskNumber := strings.Split(taskName, "_")[0]
			taskNumberInt, err := strconv.Atoi(taskNumber)
			if err != nil {
				println("Check worker status: Error in getting Task Number")
			}
			matched, _ := regexp.MatchString("map", taskName)
			workerDead := time.Since(lastActive) > 10*time.Second
			if matched && workerDead {
				m.unassignedPartitions = append(m.unassignedPartitions, taskNumberInt)
			} else if !matched && workerDead {
				m.unassignmedReducedTasks = append(m.unassignmedReducedTasks, taskNumberInt)
			}

			if workerDead {
				delete(m.Tasks, taskName)
			}
		}

	}
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
	if len(m.completedReduceTasks) == m.nReduce {
		return true
	}
	// Your code here.

	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	tasks := make(map[string]time.Time, 0)
	unassignedPartitions := make([]int, 0)
	unassignedReduceTasks := make([]int, 0)
	completedReduceTasks := make([]int, 0)
	completedMapTasks := make([]int, 0)
	for i := 0; i < len(files); i++ {
		unassignedPartitions = append(unassignedPartitions, i)
	}

	for j := 0; j < nReduce; j++ {
		unassignedReduceTasks = append(unassignedReduceTasks, j)
	}

	m := Master{files: files, nReduce: nReduce, Tasks: tasks, unassignedPartitions: unassignedPartitions, completedMapTasks: completedMapTasks, unassignmedReducedTasks: unassignedReduceTasks, completedReduceTasks: completedReduceTasks}
	print("Master running...")
	// Your code here.
	// m.CheckWorkerStatus()
	m.server()
	m.CheckWorkerStatus()
	return &m
}
