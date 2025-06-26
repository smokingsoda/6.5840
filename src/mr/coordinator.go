package mr

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const TIMEOUT = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	mMap         int
	nReduce      int
	idAndFileMap map[int]string

	mapDoneCh     chan struct{}
	reduceStartCh chan struct{}
	reduceDoneCh  chan struct{}
	doneCh        chan struct{}

	deleteCh chan int
	tasks    map[int]HeartbeatMessage
	// Lock-free file allocation channels
	coordinatorInnerCh chan chan CoordinatorInnerRequest // Channel for worker file requests
	availableFileIDs   chan int                          // Channel for available files
	availableReduceIDs chan int                          // Channel for available ids

	// counts
	mapCount    int
	reduceCount int

	heartbeat chan HeartbeatMessage

	// Mutex to protect concurrent access to counts
	mu sync.Mutex
}

type CoordinatorInnerRequest struct {
	TaskType string
	TaskID   int
}

type HeartbeatMessage struct {
	TaskType string
	TaskID   int
	SendTime time.Time //The sent time of Heartbeat message
}

type HeartbeatReply struct{}

// File allocator - implements lock-free using channels
func (c *Coordinator) mapFileAllocator() {
	for {
		select {
		case responseCh := <-c.coordinatorInnerCh:
			// Received file request from worker
			select {
			case fileID := <-c.availableFileIDs:
				// Available file found, assign to worker
				responseCh <- CoordinatorInnerRequest{"map", fileID}
				c.heartbeat <- HeartbeatMessage{TaskType: "map", TaskID: fileID, SendTime: time.Now()}
			case <-c.mapDoneCh:
				close(c.reduceStartCh)
				c.coordinatorInnerCh <- responseCh
				return
			}
		case <-c.mapDoneCh:
			close(c.reduceStartCh)
			return
		}
	}
}

func (c *Coordinator) reduceAllocator() {
	// log.Printf("Not in reduce phase yet")
	<-c.reduceStartCh // Wait for channel to be closed
	// log.Printf("****************Now in reduce phase***************")
	for {
		select {
		case responseCh := <-c.coordinatorInnerCh:
			select {
			case reduceID := <-c.availableReduceIDs:
				responseCh <- CoordinatorInnerRequest{"reduce", reduceID}
				c.heartbeat <- HeartbeatMessage{TaskType: "reduce", TaskID: reduceID, SendTime: time.Now()}
			case <-c.reduceDoneCh:
				responseCh <- CoordinatorInnerRequest{"done", -1}
				return
			}
		case <-c.reduceDoneCh:
			return
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// Create response channel
	responseCh := make(chan CoordinatorInnerRequest)

	// Request file from allocator
	c.coordinatorInnerCh <- responseCh

	// Wait for file allocation result
	innerResponse := <-responseCh

	switch innerResponse.TaskType {
	case "map":
		reply.TaskType = "map"
		reply.FileName = c.idAndFileMap[innerResponse.TaskID]
		reply.TaskID = innerResponse.TaskID
		reply.NReduce = c.nReduce
		reply.MMap = c.mMap
		// log.Printf("Assigned file %s to worker", reply.FileName)
	case "reduce":
		reply.TaskType = "reduce"
		reply.FileName = ""
		reply.TaskID = innerResponse.TaskID
		reply.NReduce = c.nReduce
		reply.MMap = c.mMap
		// log.Printf("Assigned reduceID %d to worker", reply.TaskID)
	default:
		reply.TaskType = "done"
		reply.TaskID = -1
	}

	return nil
}

func (c *Coordinator) ReplyDone(args *ReplyDoneArgs, reply *ReplyDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First remove the completed task from tracking to prevent re-assignment
	// It is important to do this
	// If we count first and then delete, there will be a awful time window
	c.deleteCh <- args.TaskID
	// Check if this task completion should be counted
	switch args.TaskType {
	case "map":
		if c.mapCount >= c.mMap {
			// All map tasks already completed, ignore duplicate completion
			// log.Printf("[%s] Ignoring duplicate map task completion: TaskID=%d (Current count: %d/%d)", time.Now().Format("15:04:05.000"), args.TaskID, c.mapCount, c.mMap)
			return nil
		}
		c.mapCount += 1
		// log.Printf("[%s] Map task %d completed, count: %d/%d", time.Now().Format("15:04:05.000"), args.TaskID, c.mapCount, c.mMap)
		if c.mapCount == c.mMap {
			// log.Printf("All map tasks completed, starting reduce phase")
			close(c.mapDoneCh)
		}
	case "reduce":
		if c.reduceCount >= c.nReduce {
			// All reduce tasks already completed, ignore duplicate completion
			// log.Printf("[%s] Ignoring duplicate reduce task completion: TaskID=%d (Current count: %d/%d)", time.Now().Format("15:04:05.000"), args.TaskID, c.reduceCount, c.nReduce)
			return nil
		}
		c.reduceCount += 1
		// log.Printf("[%s] Reduce task completed, count: %d/%d", time.Now().Format("15:04:05.000"), c.reduceCount, c.nReduce)
		if c.reduceCount == c.nReduce {
			// log.Printf("All reduce tasks completed, closing done channel")
			close(c.reduceDoneCh)
			c.doneCh <- struct{}{}
		}
	}
	return nil
}

func (c *Coordinator) HeartbeatRPC(args *HeartbeatMessage, reply *HeartbeatReply) error {
	c.heartbeat <- *args
	return nil
}

func (c *Coordinator) HeartbeatManager() {
	// Create timer to check timeout every TIMEOUT interval
	ticker := time.NewTicker(TIMEOUT)
	defer ticker.Stop()

	for {
		select {
		case h := <-c.heartbeat:
			// Received heartbeat message, update task record
			c.tasks[h.TaskID] = h
		case <-ticker.C:
			// Timer triggered, check all tasks for timeout
			curTime := time.Now()
			// log.Printf("[%s] Checking heartbeat timeouts", curTime.Format("15:04:05.000"))

			for taskID, lastHeartbeatMessage := range c.tasks {
				// Skip zero time heartbeat records
				// if lastHeartbeatMessage.SendTime.IsZero() {
				// 	log.Printf("Skipping zero time heartbeat record: TaskType=%s, TaskID=%d",
				// 		lastHeartbeatMessage.TaskType, lastHeartbeatMessage.TaskID)
				// 	continue
				// }
				if curTime.Sub(lastHeartbeatMessage.SendTime) >= TIMEOUT {
					// Detected timeout task, resend
					// log.Printf("Detected task timeout, resending task: TaskType=%s, TaskID=%d, Last heartbeat: %s, Time since: %.1f seconds",
					//	lastHeartbeatMessage.TaskType,
					//	lastHeartbeatMessage.TaskID,
					//	lastHeartbeatMessage.SendTime.Format("15:04:05.000"),
					//	curTime.Sub(lastHeartbeatMessage.SendTime).Seconds())

					switch lastHeartbeatMessage.TaskType {
					case "map":
						c.availableFileIDs <- lastHeartbeatMessage.TaskID
					case "reduce":
						c.availableReduceIDs <- lastHeartbeatMessage.TaskID
					}
					// Update task heartbeat time to avoid duplicate resends
					task := c.tasks[taskID]
					task.SendTime = curTime
					c.tasks[taskID] = task
				}
			}
		case deleteID := <-c.deleteCh:
			// log.Printf("[%s] Deleting completed task: TaskType=%s, TaskID=%d", time.Now().Format("15:04:05.000"), c.tasks[deleteID].TaskType, deleteID)
			delete(c.tasks, deleteID)
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
		// log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	select {
	case <-c.doneCh:
		return true
	default:
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Initialize channels
	c.coordinatorInnerCh = make(chan chan CoordinatorInnerRequest)
	c.availableFileIDs = make(chan int, len(files))
	c.availableReduceIDs = make(chan int, nReduce)
	c.mapDoneCh = make(chan struct{})
	c.reduceStartCh = make(chan struct{})
	c.reduceDoneCh = make(chan struct{})
	c.doneCh = make(chan struct{})
	c.deleteCh = make(chan int)
	c.mMap = len(files)
	c.nReduce = nReduce
	c.heartbeat = make(chan HeartbeatMessage, max(c.mMap, c.nReduce))
	c.idAndFileMap = make(map[int]string)
	c.tasks = make(map[int]HeartbeatMessage)
	c.mapCount = 0
	c.reduceCount = 0
	// Put all files into available files channel
	for idx, file := range files {
		c.idAndFileMap[idx] = file
		c.availableFileIDs <- idx
	}
	for i := 0; i < c.nReduce; i++ {
		c.availableReduceIDs <- i
	}
	// log.Printf("Coordinator started with %d files to assign", len(files))

	// Start file allocator goroutine
	go c.HeartbeatManager()
	go c.mapFileAllocator()
	go c.reduceAllocator()

	go c.server()
	return &c
}
