package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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
	var newJob JobReply
	var finishedJob JobArgs = JobArgs{DoneJobType: JobTypeNone}
	for {
		newJob = getJob(&finishedJob)
		switch newJob.Type {
		case JobTypeMap:
			finishedJob = handleMapJob(newJob, mapf, finishedJob)
		case JobTypeReduce:
			finishedJob = handleReduceJob(newJob, reducef, finishedJob)
		case JobTypeSleep:
			finishedJob = handleSleepJob(finishedJob)
		case JobTypeExit:
			return
		default:
			return
		}
	}
}

func handleMapJob(newJob JobReply, mapf func(string, string) []KeyValue, finishedJob JobArgs) JobArgs {
	f := newJob.Files[0]
	file, err := os.Open(f)
	if err != nil {
		log.Fatalf("cannot open %v", f)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", f)
	}

	intermediate := mapf(f, string(content))

	withReduceFiles := make(map[int][]KeyValue)
	for _, kv := range intermediate {
		idx := ihash(kv.Key) % newJob.NReduce
		withReduceFiles[idx] = append(withReduceFiles[idx], kv)
	}

	files := make([]string, newJob.NReduce)
	for rId, kvs := range withReduceFiles {
		filename := fmt.Sprintf("mr-%d-%d", newJob.Id, rId)
		ofile, _ := os.Create(filename)
		defer ofile.Close()
		enc := json.NewEncoder(ofile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		files[rId] = filename
	}

	finishedJob = JobArgs{DoneJobType: JobTypeMap, Id: newJob.Id, Files: files}

	return finishedJob
}

func handleReduceJob(newTask JobReply, reducef func(string, []string) string, finishedJob JobArgs) JobArgs {
	intermediate := []KeyValue{}

	for _, filename := range newTask.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", newTask.Id)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	finishedJob = JobArgs{DoneJobType: JobTypeReduce, Id: newTask.Id, Files: []string{oname}}

	return finishedJob
}

func handleSleepJob(finishedJob JobArgs) JobArgs {
	time.Sleep(500 * time.Millisecond)
	finishedJob = JobArgs{DoneJobType: JobTypeNone}
	return finishedJob
}

func getJob(finishedJob *JobArgs) JobReply {
	reply := JobReply{}

	success := call("Coordinator.GetJob", finishedJob, &reply)
	if !success {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
