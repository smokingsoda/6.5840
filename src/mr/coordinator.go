package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Task struct {
	fileName         string
	lastResponseTime time.Time
	status           bool
	id               int
}

type Coordinator struct {
	// Your definitions here.
	tasks   []Task
	files   []string
	nMap    int
	mReduce int

	heartbeatCh chan HeartbeatMsg
	reportCh    chan ReportMsg
	doneCh      chan struct{}

	// Lock-free file allocation channels
	fileRequestCh  chan chan string // Channel for worker file requests
	availableFiles chan string      // Channel for available files
}

type HeartbeatMsg struct {
	// Heartbeat message
	response *HeartbeatResponse
	ok       chan struct{}
}

type ReportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// Coordinator's response after receiving heartbeat from Worker
type HeartbeatResponse struct {
}

// Worker's request to report task completion status to Coordinator
type ReportRequest struct {
}

// File allocator - implements lock-free using channels
func (c *Coordinator) fileAllocator() {
	for {
		select {
		case responseCh := <-c.fileRequestCh:
			// Received file request from worker
			select {
			case fileName := <-c.availableFiles:
				// Available file found, assign to worker
				responseCh <- fileName
			default:
				// No available files, return empty string to indicate completion
				responseCh <- ""
			}
		case <-c.doneCh:
			return
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// Create response channel
	responseCh := make(chan string)

	// Request file from allocator
	c.fileRequestCh <- responseCh

	// Wait for file allocation result
	fileName := <-responseCh

	if fileName != "" {
		reply.TaskType = "map"
		reply.FileName = fileName
		log.Printf("Assigned file %s to worker", fileName)
	} else {
		reply.TaskType = "done"
		reply.FileName = ""
		log.Printf("All files have been assigned")
		c.doneCh <- struct{}{}
	}

	return nil
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
	c := Coordinator{}

	// Initialize channels
	c.fileRequestCh = make(chan chan string)
	c.availableFiles = make(chan string, len(files))
	c.doneCh = make(chan struct{})

	// Put all files into available files channel
	for _, file := range files {
		c.availableFiles <- file
	}

	log.Printf("Coordinator started with %d files to assign", len(files))

	// Start file allocator goroutine
	go c.fileAllocator()

	c.server()
	return &c
}
