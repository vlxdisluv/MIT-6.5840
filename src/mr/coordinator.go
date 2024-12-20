package mr

import (
	"fmt"
	"log"
	"sync"
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
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

type Task struct {
	TaskNumber int
	TaskType   TaskType
	FileName   string
	Status     TaskStatus
	StartTime  time.Time
}

type Coordinator struct {
	tasks      []Task
	nReduce    int
	mapDone    bool
	reduceDone bool
	taskMutex  sync.Mutex
}

func InitTasks(files []string) []Task {
	tasks := []Task{}

	for i, file := range files {
		tasks = append(tasks, Task{
			TaskNumber: i,
			TaskType:   MapTask,
			FileName:   file,
			Status:     Loaded,
		})
	}

	initialLen := len(tasks)
	for i := 0; i < initialLen; i++ {
		tasks = append(tasks, Task{
			TaskNumber: i,
			TaskType:   ReduceTask,
			Status:     Loaded,
		})
	}

	return tasks
}

func (c *Coordinator) GetTask(_ *TaskArgs, reply *TaskReply) error {
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	if c.mapDone && c.reduceDone {
		reply.Done = true
		return nil
	}

	if !c.mapDone {
		for i := range c.tasks {
			task := &c.tasks[i]
			if task.TaskType == MapTask && task.Status == Loaded {
				task.Status = InProgress
				task.StartTime = time.Now()

				reply.TaskNumber = task.TaskNumber
				reply.FileName = task.FileName
				reply.TaskType = MapTask
				reply.NReduce = c.nReduce
				reply.Done = false
				return nil
			}

			c.CoordinatorState()
		}

		c.checkMapTaskProgress()
	}

	if !c.reduceDone {
		for i := range c.tasks {
			task := &c.tasks[i]
			if task.TaskType == ReduceTask && task.Status == Loaded {
				task.Status = Processed
				task.StartTime = time.Now()

				reply.TaskNumber = task.TaskNumber
				reply.TaskType = ReduceTask
				reply.NReduce = c.nReduce
				reply.Done = false
				return nil
			}

			c.CoordinatorState()
		}

		c.checkReduceTaskProgress()
	}

	reply.Done = false
	reply.TaskType = -1
	return nil
}

func (c *Coordinator) checkMapTaskProgress() {
	for _, task := range c.tasks {
		if task.TaskType == MapTask && task.Status != Processed {
			return
		}
	}

	c.mapDone = true
}

func (c *Coordinator) checkReduceTaskProgress() {
	for _, task := range c.tasks {
		if task.TaskType == ReduceTask && task.Status != Processed {
			return
		}
	}

	c.reduceDone = true
}

func (c *Coordinator) ChangeTaskStatus(args *ChangeTaskStatusArgs, _ *ChangeTaskStatusReply) error {
	c.taskMutex.Lock()
	defer c.taskMutex.Unlock()

	for i := range c.tasks {
		task := &c.tasks[i]

		if task.TaskType == args.TaskType && task.TaskNumber == args.TaskNumber {
			task.Status = args.TaskStatus
		}
	}

	c.checkMapTaskProgress()
	c.checkReduceTaskProgress()

	return nil
}

func (s TaskStatus) string() string {
	switch s {
	case Loaded:
		return "Loaded"
	case InProgress:
		return "InProgress"
	case Processed:
		return "Processed"
	default:
		return "Unknown"
	}
}

func (c *Coordinator) CoordinatorState() {
	fmt.Println("-------------------------------")
	fmt.Println("Current state of map tasks:")
	for _, task := range c.tasks {
		if task.TaskType == MapTask {
			fmt.Printf("TaskNumber: %d, File: %s, Status: %s\n", task.TaskNumber, task.FileName, task.Status.string())
		}
	}
	fmt.Println("-------------------------------")
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
	tasks := InitTasks(files)

	c := Coordinator{
		tasks:      tasks,
		nReduce:    nReduce,
		mapDone:    false,
		reduceDone: false,
	}

	c.CoordinatorState()

	c.server()
	return &c
}
