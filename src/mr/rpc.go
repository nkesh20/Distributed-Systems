package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type JobType int

const (
	JobTypeNone   JobType = 0
	JobTypeMap    JobType = 1
	JobTypeReduce JobType = 2
	JobTypeSleep  JobType = 3
	JobTypeExit   JobType = 4
)

type JobArgs struct {
	DoneJobType JobType
	Id          int
	Files       []string
}

type JobReply struct {
	Type    JobType
	Id      int
	Files   []string
	NReduce int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
