package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type ReduceJob struct {
	jobId      int
	inputFiles []string
	startTime  time.Time
	isFinished bool
}

type MapJob struct {
	jobId      int
	inputFile  string
	startTime  time.Time
	isFinished bool
}

type Coordinator struct {
	lock            sync.Mutex
	mapJobs         []MapJob
	remainingMap    int
	reduceJobs      []ReduceJob
	remainingReduce int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetJob(args *JobArgs, reply *JobReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	switch args.DoneJobType {
	case JobTypeMap:
		if !c.mapJobs[args.Id].isFinished {
			c.mapJobs[args.Id].isFinished = true
			for reduceId, file := range args.Files {
				if len(file) > 0 {
					c.reduceJobs[reduceId].inputFiles = append(c.reduceJobs[reduceId].inputFiles, file)
				}
			}
			c.remainingMap--
		}
	case JobTypeReduce:
		if !c.reduceJobs[args.Id].isFinished {
			c.reduceJobs[args.Id].isFinished = true
			c.remainingReduce--
		}
	}

	now := time.Now()
	tenSecondsAgo := now.Add(-10 * time.Second)
	if c.remainingMap > 0 {
		for index := range c.mapJobs {
			j := &c.mapJobs[index]
			if !j.isFinished && j.startTime.Before(tenSecondsAgo) {
				reply.Type = JobTypeMap
				reply.Id = j.jobId
				reply.Files = []string{j.inputFile}
				reply.NReduce = len(c.reduceJobs)

				j.startTime = now

				return nil
			}
		}
		reply.Type = JobTypeSleep
	} else if c.remainingReduce > 0 {
		for index := range c.reduceJobs {
			j := &c.reduceJobs[index]
			if !j.isFinished && j.startTime.Before(tenSecondsAgo) {
				reply.Type = JobTypeReduce
				reply.Id = j.jobId
				reply.Files = j.inputFiles

				j.startTime = now

				return nil
			}
		}
		reply.Type = JobTypeSleep
	} else {
		reply.Type = JobTypeExit
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.remainingMap == 0 && c.remainingReduce == 0

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapJobs:         make([]MapJob, len(files)),
		reduceJobs:      make([]ReduceJob, nReduce),
		remainingMap:    len(files),
		remainingReduce: nReduce,
	}
	for i, f := range files {
		c.mapJobs[i] = MapJob{jobId: i, inputFile: f, isFinished: false}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceJobs[i] = ReduceJob{jobId: i, isFinished: false}
	}

	c.server()
	return &c
}
