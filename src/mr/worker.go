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
	Debug(dHeartbeat, "Starting heartbeat sender for %s task %d", *taskType, *taskID)
	ticker := time.NewTicker(TIMEOUT / 2)
	defer ticker.Stop()

	for {
		select {
		case <-doneCh:
			Debug(dHeartbeat, "Stopping heartbeat sender for %s task %d", *taskType, *taskID)
			return // Exit heartbeat sender when task is done
		case <-ticker.C:
			// Create a local copy to avoid race conditions
			localTaskType := *taskType
			localTaskID := *taskID
			sendTime := time.Now()
			args := HeartbeatMessage{TaskType: localTaskType, TaskID: localTaskID, SendTime: sendTime}
			reply := HeartbeatReply{}
			if args.TaskType == "done" {
				Debug(dHeartbeat, "Task marked as done, stopping heartbeat")
				return // Exit heartbeat sender when done
			}
			ok := call("Coordinator.HeartbeatRPC", &args, &reply)
			if !ok {
				Debug(dError, "Heartbeat failed, coordinator may be down")
				return
			}
			DebugLevel(3, dHeartbeat, "Sent heartbeat for %s task %d", localTaskType, localTaskID)
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	Debug(dWorker, "Worker started, requesting tasks...")

	// Your worker implementation here.
	for {
		var taskID int
		var taskType string
		startCh := make(chan struct{})
		doneCh := make(chan struct{})
		go HeartbeatSender(&taskType, &taskID, startCh, doneCh)

		Debug(dWorker, "Requesting new task from coordinator")
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)

		if !ok {
			Debug(dError, "Cannot connect to coordinator, worker exiting")
			return
		}

		if reply.TaskType == "done" {
			Debug(dWorker, "All tasks completed, worker exiting")
			return
		}

		taskID = reply.TaskID
		taskType = reply.TaskType
		startCh <- struct{}{}

		if reply.TaskType == "map" {
			Debug(dMap, "Received map task %d (file: %s)", reply.TaskID, reply.FileName)

			// Read input file
			content, err := ioutil.ReadFile(reply.FileName)
			if err != nil {
				Debug(dError, "Error reading file %s: %v", reply.FileName, err)
				doneCh <- struct{}{}
				continue
			}
			Debug(dFile, "Read file %s (%d bytes)", reply.FileName, len(content))

			mapTaskId := reply.TaskID
			fileContent := string(content)
			kva := mapf(reply.FileName, fileContent)
			Debug(dMap, "Map function produced %d key-value pairs", len(kva))

			// Create intermediate files
			intermediateFiles := make([]*os.File, reply.NReduce)
			encoder := make([]*json.Encoder, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				filename := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
				file, err := os.Create(filename)
				if err != nil {
					Debug(dError, "Cannot create intermediate file %s: %v", filename, err)
					doneCh <- struct{}{}
					continue // Skip this task
				}
				defer file.Close()
				intermediateFiles[i] = file
				encoder[i] = json.NewEncoder(file)
			}
			Debug(dFile, "Created %d intermediate files for map task %d", reply.NReduce, reply.TaskID)

			// Write key-value pairs to intermediate files
			kvCounts := make([]int, reply.NReduce)
			for _, kv := range kva {
				reduceIdx := ihash(kv.Key) % reply.NReduce
				err := encoder[reduceIdx].Encode(&kv)
				if err != nil {
					Debug(dError, "Cannot write to intermediate file: %v", err)
					break
				}
				kvCounts[reduceIdx]++
			}

			for i, count := range kvCounts {
				DebugLevel(2, dFile, "Intermediate file mr-%d-%d contains %d key-value pairs", reply.TaskID, i, count)
			}

			// Reply to coordinator
			args := ReplyDoneArgs{"map", mapTaskId}
			reply := ReplyDoneReply{}
			call("Coordinator.ReplyDone", &args, &reply)
			Debug(dMap, "Map task %d completed successfully", mapTaskId)
			doneCh <- struct{}{}

		} else if reply.TaskType == "reduce" {
			Debug(dReduce, "Received reduce task %d", reply.TaskID)

			reduceTaskID := reply.TaskID
			intermediate := []KeyValue{}

			// Read intermediate files
			filesRead := 0
			totalKVs := 0
			for mapTaskID := 0; mapTaskID < reply.MMap; mapTaskID++ {
				filename := fmt.Sprintf("mr-%d-%d", mapTaskID, reduceTaskID)

				// Check if file exists
				if file, err := os.Open(filename); err == nil {
					filesRead++
					decoder := json.NewDecoder(file)
					fileKVs := 0

					// Read all k-v pairs
					for {
						var kv KeyValue
						if err := decoder.Decode(&kv); err != nil {
							break // finish read or error
						}
						intermediate = append(intermediate, kv)
						fileKVs++
						totalKVs++
					}
					defer file.Close()
					DebugLevel(2, dFile, "Read %d key-value pairs from %s", fileKVs, filename)
				} else {
					DebugLevel(2, dFile, "Intermediate file %s not found (normal if map task had no output for this partition)", filename)
				}
			}
			Debug(dReduce, "Read total %d key-value pairs from %d intermediate files", totalKVs, filesRead)

			// Sort by key
			sort.Slice(intermediate, func(i, j int) bool {
				return intermediate[i].Key < intermediate[j].Key
			})
			Debug(dReduce, "Sorted %d key-value pairs", len(intermediate))

			// Create output file
			outputFilename := fmt.Sprintf("mr-out-%d", reduceTaskID)
			outputFile, err := os.Create(outputFilename)
			if err != nil {
				Debug(dError, "Cannot create output file %s: %v", outputFilename, err)
				doneCh <- struct{}{}
				continue // Skip this task and request next one
			}
			defer outputFile.Close()

			// Apply reduce function
			i := 0
			keysProcessed := 0
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
				keysProcessed++
				i = j
			}
			Debug(dReduce, "Processed %d unique keys, output written to %s", keysProcessed, outputFilename)

			args := ReplyDoneArgs{"reduce", reduceTaskID}
			reply := ReplyDoneReply{}
			call("Coordinator.ReplyDone", &args, &reply)
			Debug(dReduce, "Reduce task %d completed successfully", reduceTaskID)
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
		Debug(dError, "RPC dial error: %v", err)
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	DebugLevel(3, dRPC, "Calling %s", rpcname)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		DebugLevel(3, dRPC, "RPC call %s succeeded", rpcname)
		return true
	}

	Debug(dError, "RPC call %s failed: %v", rpcname, err)
	fmt.Println(err)
	return false
}
