package mr

import "os"
import "strconv"

type TaskArgs struct {
}

type TaskReply struct {
	FileName   string
	TaskType   TaskType
	TaskNumber int
	NReduce    int
	Done       bool
}

type ChangeTaskStatusArgs struct {
	TaskType   TaskType
	TaskNumber int
	TaskStatus TaskStatus
}

type ChangeTaskStatusReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
