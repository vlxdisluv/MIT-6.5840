package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int

const (
	Loaded TaskStatus = iota
	InProgress
	Processed
	Failed
)

type TaskInfo struct {
	TaskNumber int
	Status     TaskStatus
	StartTime  time.Time
}

type MapTaskInfo struct {
	FileName string
	TaskInfo
}

type ReduceTaskInfo struct {
	TaskInfo
}

type Coordinator struct {
	mapTasks    []*MapTaskInfo
	reduceTasks []*ReduceTaskInfo
	nReduce     int
	mapDone     bool
	reduceDone  bool
}

func (c *Coordinator) GetTask(_ *TaskArgs, reply *TaskReply) error {
	mapTask, mapErr := c.getNextMapTask()

	if mapErr != nil {
		return mapErr
	}

	reply.TaskNumber = mapTask.TaskNumber
	reply.FileName = mapTask.FileName
	reply.TaskType = Map
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, _ *CompleteTaskReply) error {
	var err error

	switch args.TaskType {
	case Map:
		err = c.updateTaskStatus(Map, args.TaskNumber, Processed)
	case Reduce:
		err = c.updateTaskStatus(Reduce, args.TaskNumber, Processed)
	default:
		err = fmt.Errorf("unknown task type: %v", args.TaskType)
	}

	return err
}

func (c *Coordinator) updateTaskStatus(taskType TaskType, taskNumber int, newStatus TaskStatus) error {
	if taskType == Map {
		c.mapTasks[taskNumber].Status = newStatus
		return nil
	}

	if taskType == Reduce {
		c.reduceTasks[taskNumber].Status = newStatus
		return nil
	}

	return fmt.Errorf("task type %v %v not found", taskType, taskNumber)
}

func (c *Coordinator) getNextMapTask() (mapTask *MapTaskInfo, err error) {
	for _, task := range c.mapTasks {
		if task.Status == Loaded || task.Status == Failed {
			task.Status = InProgress

			c.MapState()
			return task, nil
		}
	}

	c.MapState()
	return nil, fmt.Errorf("no map task available")
}

func (s TaskStatus) string() string {
	switch s {
	case Loaded:
		return "Loaded"
	case InProgress:
		return "InProgress"
	case Processed:
		return "Processed"
	case Failed:
		return "Failed"
	default:
		return "Unknown"
	}
}

func (c *Coordinator) MapState() {
	fmt.Println("-------------------------------")
	fmt.Println("Current state of map tasks:")
	for _, mapTask := range c.mapTasks {
		fmt.Printf("TaskNumber: %d, File: %s, Status: %s\n", mapTask.TaskNumber, mapTask.FileName, mapTask.Status.string())
	}
	fmt.Println("-------------------------------")
}

func InitMapTasks(fileNames []string) []*MapTaskInfo {
	mapTasks := []*MapTaskInfo{}

	for index, fileName := range fileNames {
		mapTask := &MapTaskInfo{
			FileName: fileName,
			TaskInfo: TaskInfo{
				TaskNumber: index,
				Status:     Loaded,
				StartTime:  time.Now(),
			},
		}

		mapTasks = append(mapTasks, mapTask)
	}

	return mapTasks
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := InitMapTasks(files)

	c := Coordinator{
		mapTasks:    mapTasks,
		reduceTasks: []*ReduceTaskInfo{},
		nReduce:     nReduce,
		mapDone:     false,
		reduceDone:  false,
	}

	c.server()
	return &c
}
