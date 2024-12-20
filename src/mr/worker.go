package mr

import (
	"encoding/json"
	"fmt"
	"os"
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

		switch taskReply.TaskType {
		case MapTask:
			err = handleMapTask(taskReply, mapf)
		case ReduceTask:
			err = fmt.Errorf("reduce handler is not implemented")
		default:
			fmt.Printf("unknown task type: %v\n", taskReply.TaskType)
			err = fmt.Errorf("unsupported task type")
		}

		if err != nil {
			fmt.Printf("error processing task %v: %v\n", taskReply.TaskType, err)
			break
		}

	}
}

func GetTask() (*TaskReply, error) {
	args := &TaskArgs{}
	reply := &TaskReply{}

	ok := call("Coordinator.GetTask", args, reply)

	if !ok {
		return nil, fmt.Errorf("task not found")
	}

	return reply, nil
}

func ChangeTaskStatus(taskType TaskType, taskNumber int, taskStatus TaskStatus) (*ChangeTaskStatusReply, error) {
	args := &ChangeTaskStatusArgs{
		taskType,
		taskNumber,
		taskStatus,
	}
	reply := &ChangeTaskStatusReply{}

	ok := call("Coordinator.ChangeTaskStatus", args, reply)

	if !ok {
		return nil, fmt.Errorf("task not found")
	}

	return reply, nil
}

func handleMapTask(taskReply *TaskReply, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(taskReply.FileName)
	if err != nil {
		return fmt.Errorf("cannot open %v: %v", taskReply.FileName, err)
	}

	kva := mapf(taskReply.FileName, string(content))
	fmt.Printf("Processing file with Map task: %v\n", len(kva))

	encoders := make([]*json.Encoder, taskReply.NReduce)
	for i := 0; i < taskReply.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", taskReply.TaskNumber, i)

		outputFile, err := os.Create(filename)
		defer outputFile.Close()

		if err != nil {
			return fmt.Errorf("cannot create file %v: %v", filename, err)
		}

		encoders[i] = json.NewEncoder(outputFile)
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % taskReply.NReduce
		if err := encoders[bucket].Encode(&kv); err != nil {
			return fmt.Errorf("cannot write to intermediate file: %v", err)
		}
	}

	time.Sleep(5 * time.Second)

	if _, err := ChangeTaskStatus(taskReply.TaskType, taskReply.TaskNumber, Processed); err != nil {
		return fmt.Errorf("cannot complete task: %v", err)
	}

	return nil
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
