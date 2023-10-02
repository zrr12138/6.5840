package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	MapTaskType TaskType = iota
	ReduceTaskType
	NoTask
)
const noWork = -1

type MapTaskResponse struct {
	TaskId    int
	Filename  string
	ReduceNum int
}

type ReduceTaskResponse struct {
	TaskId     int
	MapTaskNum int
}

type ApplyTaskRequest struct {
	WorkerId uint64
}
type ApplyTaskResponse struct {
	Type       TaskType
	MapTask    *MapTaskResponse
	ReduceTask *ReduceTaskResponse
}

type FinishTaskRequest struct {
	WorkerId uint64
	Type     TaskType
	TaskId   int
}
type FinishTaskResponse struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
