package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "math/rand"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var WorkerId uint64

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type RpcFailError struct {
}

func (e RpcFailError) Error() string {
	return "rpc failed"
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	rand.Seed(time.Now().UnixNano() + int64(os.Getpid()))
	WorkerId = rand.Uint64()
	log.Printf("work start with id:%v", WorkerId)
	// Your worker implementation here.
	for {
		response, err := ApplyTask()
		if err != nil {
			fmt.Printf(err.Error())
			time.Sleep(1 * time.Second)
			continue
		}
		if response.Type == MapTaskType {
			mapTask := response.MapTask
			log.Printf("get map task type, id=%v,filename=%v", mapTask.TaskId, mapTask.Filename)
			err = HandleMapTask(mapTask, mapf)
			if err != nil {
				log.Printf("handle map task error, err=%v", err)
				continue
			}
			err = HandleFinishTask(MapTaskType, response.MapTask.TaskId)
			if err != nil {
				log.Printf("handle map task finish error, err=%v,just ignore it", err)
				continue
			}
		} else if response.Type == ReduceTaskType {
			reduceTask := response.ReduceTask
			log.Printf("get reduce task type, id=%v", reduceTask.TaskId)
			err = HandleReduceTask(reduceTask, reducef)
			if err != nil {
				log.Printf("handle reduce task error, err=%v", err)
				continue
			}
			err = HandleFinishTask(ReduceTaskType, response.ReduceTask.TaskId)
			if err != nil {
				log.Printf("handle reduce task finish error, err=%v,just ignore it", err)
				continue
			}
		} else if response.Type == NoTask {
			fmt.Println("get no task type, exit now")
			break
		} else {
			fmt.Println("unknown task type")
			break
		}
	}

}
func HandleFinishTask(Type TaskType, taskId int) error {
	log.Printf("send rpc finish task type:%v id:%v", Type, taskId)
	request := FinishTaskRequest{
		WorkerId: WorkerId,
		Type:     Type,
		TaskId:   taskId,
	}
	response := FinishTaskResponse{}
	ok := call("Coordinator.FinishTask", &request, &response)
	if !ok {
		fmt.Printf("call failed!\n")
		return RpcFailError{}
	}
	return nil
}
func getValuesArray(kvs []KeyValue) []string {
	var result []string
	for _, kv := range kvs {
		result = append(result, kv.Value)
	}
	return result
}
func HandleReduceTask(response *ReduceTaskResponse, reducef func(string, []string) string) error {
	var kva []KeyValue
	for i := 0; i < response.MapTaskNum; i++ {
		fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(response.TaskId)
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	tempOutFileName := strconv.FormatUint(WorkerId, 10) + "-out-temp"
	tempFile, err := os.Create(tempOutFileName)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(tempFile)
	sort.Sort(ByKey(kva))
	j := 0
	for i := 0; i < len(kva); i++ {
		if i > 0 && kva[i].Key != kva[i-1].Key {
			value := reducef(kva[i-1].Key, getValuesArray(kva[j:i]))
			_, err := writer.WriteString(fmt.Sprintf("%v %v\n", kva[i-1].Key, value))
			if err != nil {
				log.Printf("reduce task write temp file error,err=%v", err)
				return err
			}
			j = i
		}
	}
	if len(kva) > 0 {
		value := reducef(kva[len(kva)-1].Key, getValuesArray(kva[j:]))
		_, err = writer.WriteString(fmt.Sprintf("%v %v\n", kva[len(kva)-1].Key, value))
		if err != nil {
			log.Printf("reduce task write temp file error,err=%v", err)
			return err
		}
	}
	err = writer.Flush()
	if err != nil {
		return err
	}
	err = tempFile.Close()
	if err != nil {
		return err
	}
	err = os.Rename(tempOutFileName, "mr-out-"+strconv.Itoa(response.TaskId))
	if err != nil {
		return err
	}
	return nil

}
func HandleMapTask(response *MapTaskResponse, mapf func(string, string) []KeyValue) error {
	content, err := os.ReadFile(response.Filename)
	if err != nil {
		return err
	}
	kvs := mapf(response.Filename, string(content))

	tempFilePrefix := "mr-" + strconv.Itoa(response.TaskId) + "-"
	var enc []*json.Encoder
	var files []*os.File
	for i := 0; i < response.ReduceNum; i++ {
		file, err := os.Create(tempFilePrefix + strconv.Itoa(i))
		if err != nil {
			return err
		}
		enc = append(enc, json.NewEncoder(file))
		files = append(files, file)
	}
	for _, kv := range kvs {
		enc[ihash(kv.Key)%response.ReduceNum].Encode(&kv)
	}
	for _, file := range files {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
func ApplyTask() (*ApplyTaskResponse, error) {
	request := ApplyTaskRequest{WorkerId: WorkerId}
	response := ApplyTaskResponse{}
	ok := call("Coordinator.ApplyTask", &request, &response)
	if !ok {
		fmt.Printf("call failed!\n")
		return nil, RpcFailError{}
	}
	return &response, nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

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
