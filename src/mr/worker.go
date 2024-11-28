package mr

import (
	"fmt"
	"os"
	"sort"
	"time"
)

import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		taskReply, err := GetTask()

		if err != nil {
			fmt.Printf("error getting file name: %v\n", err)
			break
		}

		content, err := os.ReadFile(taskReply.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", taskReply.FileName)
		}
		kva := mapf(taskReply.FileName, string(content))

		fmt.Printf("Processing file: %v\n", len(kva))

		sort.Sort(ByKey(kva))
		
		time.Sleep(5 * time.Second)
	}
}

func GetTask() (*GetTaskReply, error) {
	args := &GetTaskArgs{}
	reply := &GetTaskReply{}

	ok := call("Coordinator.GetTask", args, reply)

	if !ok {
		return nil, fmt.Errorf("task not found")
	}

	return reply, nil
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
