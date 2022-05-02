package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// this is used for worker register to master
// the master will know the worker and reply an woker id
type WorkerRegisterID struct {
	WorkerID int
}
type WorkerRegisterArgs struct {
}

// used for acquire a Task

type WhichWorker struct {
	WorkerID int
}
type ReplyTask struct {
	Task *Task
}

// NoticeArgs used for notice the master the Task Done or err
type NoticeArgs struct {
	Done     bool
	TaskID   int
	Stage    taskStage
	WorkerID int
}
type ReplyArgs struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
