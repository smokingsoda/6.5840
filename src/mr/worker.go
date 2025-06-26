package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	TaskType string
	FileName string
	MMap     int
	NReduce  int
	TaskID   int
}

type ReplyDoneArgs struct {
	TaskType string
	TaskID   int
}

type ReplyDoneReply struct {
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func HeartbeatSender(taskType *string, taskID *int, startCh chan struct{}, doneCh chan struct{}) {
	<-startCh
	ticker := time.NewTicker(TIMEOUT / 2)
	defer ticker.Stop()

	for {
		select {
		case <-doneCh:
			return // Exit heartbeat sender when task is done
		case <-ticker.C:
			// Create a local copy to avoid race conditions
			localTaskType := *taskType
			localTaskID := *taskID
			sendTime := time.Now()
			args := HeartbeatMessage{TaskType: localTaskType, TaskID: localTaskID, SendTime: sendTime}
			reply := HeartbeatReply{}
			if args.TaskType == "done" {
				return // Exit heartbeat sender when done
			}
			ok := call("Coordinator.HeartbeatRPC", &args, &reply)
			if !ok {
				// fmt.Printf("Heartbeat failed, coordinator may be down\n")
				return
			}
			// log.Printf("[%s] Sending heartbeat: %s, TaskID=%d", sendTime.Format("15:04:05.000"), *taskType, *taskID)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// fmt.Printf("Worker started, requesting tasks...\n")
	for {
		var taskID int
		var taskType string
		startCh := make(chan struct{})
		doneCh := make(chan struct{})
		go HeartbeatSender(&taskType, &taskID, startCh, doneCh)
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)

		if !ok {
			// fmt.Printf("Cannot connect to coordinator, exiting\n")
			return
		}

		if reply.TaskType == "done" {
			// fmt.Printf("All tasks completed, worker exiting\n")
			return
		}
		taskID = reply.TaskID
		taskType = reply.TaskType
		startCh <- struct{}{}
		if reply.TaskType == "map" {
			// fmt.Printf("🎉 Received map task! taskID: %d\n", reply.TaskID)
			// Add actual map task processing // logic here
			content, err := ioutil.ReadFile(reply.FileName)
			if err != nil {
				// log.Printf("Error reading file %s: %v", reply.FileName, err)
				continue
			}
			mapTaskId := reply.TaskID
			fileContent := string(content)
			kva := mapf(reply.FileName, fileContent)
			intermediateFiles := make([]*os.File, reply.NReduce)
			encoder := make([]*json.Encoder, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				filename := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
				file, err := os.Create(filename)
				if err != nil {
					// log.Printf("Cannot create file %s", filename)
					continue // Skip this task
				}
				defer file.Close()
				intermediateFiles[i] = file
				encoder[i] = json.NewEncoder(file)
			}
			for _, kv := range kva {
				reduceIdx := ihash(kv.Key) % reply.NReduce
				err := encoder[reduceIdx].Encode(&kv)
				if err != nil {
					// log.Printf("Cannot write to intermediate file")
					break
				}
			}
			// Reply to coordinator
			args := ReplyDoneArgs{"map", mapTaskId}
			reply := ReplyDoneReply{}
			call("Coordinator.ReplyDone", &args, &reply)
			// fmt.Printf("✅ Map task %d completed\n", mapTaskId)
			doneCh <- struct{}{}
		} else if reply.TaskType == "reduce" {
			// Do reduce
			// fmt.Printf("🔄 Received reduce task! ReduceID: %d\n", reply.TaskID)
			// Collect all intermediate files which belongs to this reduce task
			reduceTaskID := reply.TaskID
			intermediate := []KeyValue{}

			// Read files
			// Format: mr-<mapID>-<reduceID>
			for mapTaskID := 0; mapTaskID < reply.MMap; mapTaskID++ {
				filename := fmt.Sprintf("mr-%d-%d", mapTaskID, reduceTaskID)

				// Check if file exists
				if file, err := os.Open(filename); err == nil {
					decoder := json.NewDecoder(file)

					// Read all k-v pairs
					for {
						var kv KeyValue
						if err := decoder.Decode(&kv); err != nil {
							break // finish read or error
						}
						intermediate = append(intermediate, kv)
					}
					defer file.Close()
				}
			}
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})
			// Create output file
			outputFilename := fmt.Sprintf("mr-out-%d", reduceTaskID)
			outputFile, err := os.Create(outputFilename)
			if err != nil {
				// log.Printf("Cannot create output file %s", outputFilename)
				continue // Skip this task and request next one
			}
			defer outputFile.Close()
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				// Collect all the values for this key
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
				i = j

			}
			// fmt.Printf("✅ Reduce task %d completed, output written to %s\n", reduceTaskID, outputFilename)
			args := ReplyDoneArgs{"reduce", reduceTaskID}
			reply := ReplyDoneReply{}
			call("Coordinator.ReplyDone", &args, &reply)
			doneCh <- struct{}{}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
