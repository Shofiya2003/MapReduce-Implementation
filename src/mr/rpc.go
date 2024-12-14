package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskRequestArgs struct {
}

type TaskRequestReply struct {
	Timestamp  time.Time
	File       string
	NReduce    int
	TaskNumber int
	Task       string
	MTasks     int
}

type TaskCompleteArgs struct {
	Timestamp  time.Time
	File       string
	TaskNumber int
	Task       string
}

type TaskCompleteReply struct {
}

type TaskFailArgs struct {
	TaskNumber int
	Task       string
}

type TaskFailReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
