package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type WorkerRequest struct{
	WorkerId string
	WorkDone int
}
//发送给worker任务Job的状态
type JobCondition int
const (
	JobWorking = iota
	JobWaiting
	JobDone
)

//type of job
type JobType int
const (
	MapJob = iota
	ReduceJob
	WaittingJob
	KillJob
)
//condition of coordinator
type Condition int
const (
	MapPhase = iota
	ReducePhase
	AllDone
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
