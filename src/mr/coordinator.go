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
import "sync/atomic"

type TaskCtx struct {
	id        int
	isStart   bool
	startTime time.Time
	isFinish  bool
	workerId  uint64
}

func GetInitTaskCtx(id int) *TaskCtx {
	return &TaskCtx{
		id:       id,
		isStart:  false,
		isFinish: false,
	}
}

type MapTaskCtx struct {
	TaskCtx
	filename string
}
type ReduceTaskCtx struct {
	TaskCtx
}
type Coordinator struct {
	// Your definitions here.
	muxLock     sync.Mutex
	mapTasks    []MapTaskCtx
	reduceTasks []ReduceTaskCtx
	nReduce     int
	isFinish    atomic.Bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//	func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//		reply.Y = args.X + 1
//		return nil
//	}
func (c *Coordinator) IsMapWorkFinish() bool {
	for _, mapTask := range c.mapTasks {
		if !mapTask.isFinish {
			return false
		}
	}
	return true
}
func (c *Coordinator) IsReduceWorkFinish() bool {
	for _, reduceTask := range c.reduceTasks {
		if !reduceTask.isFinish {
			return false
		}
	}
	return true
}

type NoTaskAvailable struct {
}

func (e *NoTaskAvailable) Error() string {
	return "there is no available map task"
}
func (c *Coordinator) ApplyMapTask() (*MapTaskCtx, error) {
	for i := range c.mapTasks {
		if c.mapTasks[i].isFinish {
			continue
		}
		if !c.mapTasks[i].isStart || time.Now().Sub(c.mapTasks[i].startTime) > 10*time.Second {
			return &c.mapTasks[i], nil
		}
	}
	return nil, &NoTaskAvailable{}
}
func (c *Coordinator) ApplyReduceTask() (*ReduceTaskCtx, error) {
	for i := range c.reduceTasks {
		if c.reduceTasks[i].isFinish {
			continue
		}
		if !c.reduceTasks[i].isStart || time.Now().Sub(c.reduceTasks[i].startTime) > 10*time.Second {
			return &c.reduceTasks[i], nil
		}
	}
	return nil, &NoTaskAvailable{}
}
func (c *Coordinator) FinishTask(request *FinishTaskRequest, response *FinishTaskResponse) error {
	c.muxLock.Lock()
	defer c.muxLock.Unlock()
	log.Printf("receive rpc finish task from %v ,finish task request %v", request.WorkerId, request)
	if request.Type == MapTaskType {
		c.mapTasks[request.TaskId].isFinish = true
	} else if request.Type == ReduceTaskType {
		c.reduceTasks[request.TaskId].isFinish = true
	}
	return nil
}
func (c *Coordinator) ApplyTask(request *ApplyTaskRequest, response *ApplyTaskResponse) error {
	c.muxLock.Lock()
	defer c.muxLock.Unlock()
	log.Printf("receive apply task request from %v", request.WorkerId)
	for !c.IsMapWorkFinish() {
		mapTaskCtx, err := c.ApplyMapTask()
		if _, ok := err.(*NoTaskAvailable); ok {
			c.muxLock.Unlock()
			log.Printf("no map task available now, wait 1s and try again,id=%v", request.WorkerId)
			time.Sleep(1 * time.Second)
			c.muxLock.Lock()
		} else {

			response.Type = MapTaskType
			response.MapTask = &MapTaskResponse{
				TaskId:    mapTaskCtx.id,
				Filename:  mapTaskCtx.filename,
				ReduceNum: c.nReduce,
			}
			mapTaskCtx.isStart = true
			mapTaskCtx.startTime = time.Now()
			log.Printf("response map task id:%v,filename:%v to %v", mapTaskCtx.id, mapTaskCtx.filename, request.WorkerId)
			return nil
		}
	}
	log.Println("map task is all finish")
	for !c.IsReduceWorkFinish() {
		reduceTaskCtx, err := c.ApplyReduceTask()
		if _, ok := err.(*NoTaskAvailable); ok {
			c.muxLock.Unlock()
			log.Printf("no reduce task available now, wait 1s and try again,id=%v", request.WorkerId)
			time.Sleep(1 * time.Second)
			c.muxLock.Lock()
		} else {
			response.Type = ReduceTaskType
			response.ReduceTask = &ReduceTaskResponse{
				TaskId:     reduceTaskCtx.id,
				MapTaskNum: len(c.mapTasks),
			}
			reduceTaskCtx.isStart = true
			reduceTaskCtx.startTime = time.Now()
			log.Printf("response reduce task id:%v to %v", reduceTaskCtx.id, request.WorkerId)
			return nil
		}
	}
	log.Println("reduce task is all finish")
	c.isFinish.Store(true)
	response.Type = NoTask
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
	return c.isFinish.Load()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.isFinish.Store(false)
	for id, file := range files {
		c.mapTasks = append(c.mapTasks, MapTaskCtx{
			TaskCtx:  *GetInitTaskCtx(id),
			filename: file,
		})
	}
	c.nReduce = nReduce
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, ReduceTaskCtx{
			*GetInitTaskCtx(i),
		})
	}
	log.Printf("coordinator init finish, maptask num:%v, reduce task num:%v", len(c.mapTasks), len(c.reduceTasks))
	c.server()
	return &c
}
