package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Implementation of a concurrent queue
type Queue struct {
	mutex sync.Mutex
	queue []int
}

func (q *Queue) Enqueue(item int) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.queue = append(q.queue, item)
}

func (q *Queue) Dequeue() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.queue) == 0 {
		return -1
	}
	ret := q.queue[0]
	q.queue = q.queue[1:]

	return ret
}

func (q *Queue) Len() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return len(q.queue)
}

func (q *Queue) Display() {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	fmt.Println(q.queue)
}

// Coordinator
type Coordinator struct {
	nMaps    int
	nReduces int
	files    []string

	mapTaskQueue     *Queue
	nFinishedMapTask int
	finishedMaps     map[int]bool

	reduceTaskQueue     *Queue
	nFinishedReduceTask int
	finishedReduces     map[int]bool
	counterMutex        sync.RWMutex // Protect 6 vars above

	taskEpoch     []int
	taskLiveness  []int64
	livenessMutex sync.RWMutex // Protect 2 vars above
}

const (
	tMap    = 0
	tReduce = 1
)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) FetchTask(request *GeneralCarrier, reply *GeneralCarrier) error {
	c.counterMutex.RLock()
	nFinishedMapTask := c.nFinishedMapTask
	nFinishedReduceTask := c.nFinishedReduceTask
	c.counterMutex.RUnlock()

	reply.Id = 0
	reply.Msg = ""
	reply.NMaps = c.nMaps
	reply.NReduces = c.nReduces

	// fmt.Printf("[Coordinator.FetchTask] nFinMap = %d, nFinReduce = %d\n", nFinishedMapTask, nFinishedReduceTask)

	c.livenessMutex.RLock()
	defer c.livenessMutex.RUnlock()

	// Dispatch map first
	taskId := c.mapTaskQueue.Dequeue()
	for c.finishedMaps[taskId] {
		taskId = c.mapTaskQueue.Dequeue()
	}
	if taskId >= 0 {
		// Dispatch map task
		reply.Kind = MsgMapTask
		reply.Id = taskId + c.taskEpoch[taskId]*c.nMaps
		reply.Msg = c.files[taskId]
		go Monitor(c, tMap, taskId)
		return nil
	} else if nFinishedMapTask < c.nMaps {
		// Ask worker to wait
		reply.Kind = MsgWaitTask
		return nil
	}

	// If no map, dispatch reduces
	taskId = c.reduceTaskQueue.Dequeue()
	for c.finishedReduces[taskId] {
		taskId = c.reduceTaskQueue.Dequeue()
	}
	if taskId >= 0 {
		// Dispatch reduce task
		reply.Kind = MsgReduceTask
		reply.Id = taskId + c.taskEpoch[c.nMaps+taskId]*c.nReduces
		go Monitor(c, tReduce, taskId)
	} else if nFinishedReduceTask < c.nReduces {
		// Ask worker to wait
		reply.Kind = MsgWaitTask
	} else {
		// Tell worker all the tasks are done
		reply.Kind = MsgNoTask
	}
	return nil
}

func (c *Coordinator) Report(request *GeneralCarrier, reply *GeneralCarrier) error {
	c.counterMutex.Lock()
	defer c.counterMutex.Unlock()
	// fmt.Printf("[Coordinator] counterMutex fetched")

	c.livenessMutex.Lock()
	defer c.livenessMutex.Unlock()
	// fmt.Printf("[Coordinator] livenessMutex fetched")

	reply.Kind = MsgOK
	if request.Kind == MsgMapFinished {
		id := request.Id % c.nMaps
		if request.Id != c.taskEpoch[id]*c.nMaps+id {
			// fmt.Printf("[Coordinator.Warning] ignoring report from consider-dead mapper %d epoch %d",
			// 	id, request.Id/c.nMaps)
			return nil
		}
		c.taskLiveness[id] = -1
		c.nFinishedMapTask++
		c.finishedMaps[id] = true
		// fmt.Printf("[Coordinator] map task %d finished (total %d)\n", request.Id, c.nFinishedMapTask)
	} else if request.Kind == MsgReduceFinished {
		id := request.Id % c.nReduces
		if request.Id != c.taskEpoch[c.nMaps+id]*c.nReduces+id {
			// fmt.Printf("[Coordinator.Warning] ignoring report from consider-dead reducer %d epoch %d",
			// 	id, request.Id/c.nReduces)
			return nil
		}
		c.taskLiveness[c.nMaps+id] = -1
		c.nFinishedReduceTask++
		c.finishedReduces[id] = true
		// fmt.Printf("[Coordinator] reduce task %d finished (total %d)\n", request.Id, c.nFinishedReduceTask)
	}
	return nil
}

func (c *Coordinator) Heartbeat(request *GeneralCarrier, reply *GeneralCarrier) error {
	c.livenessMutex.Lock()
	defer c.livenessMutex.Unlock()

	kind := request.Kind
	id := request.Id

	// fmt.Printf("[Coordinator.Heartbeat] received from task %d id %d\n", kind, id)
	if kind == tMap {
		id = id % c.nMaps
		c.taskLiveness[id] = time.Now().UnixNano()
	} else {
		id = id % c.nReduces
		c.taskLiveness[c.nMaps+id] = time.Now().UnixNano()
	}

	reply.Kind = MsgOK
	return nil
}

func Monitor(c *Coordinator, taskType int, id int) {
	timeout := time.Second.Nanoseconds()
	// var cur *int64
	c.livenessMutex.Lock()
	c.taskLiveness[taskType*c.nMaps+id] = 0
	c.livenessMutex.Unlock()
	epoch := &c.taskEpoch[taskType*c.nMaps+id]

	var failedTimes = 0

	for {
		var local int64 = 0
		for i := 0; i < 10; i++ { // Wait for the first heartbeat
			c.livenessMutex.RLock()
			local = c.taskLiveness[taskType*c.nMaps+id]
			c.livenessMutex.RUnlock()

			if local != 0 {
				break
			}
			time.Sleep(time.Millisecond * 100)
		}

		if local < 0 { // Task finished
			break
		}
		if local != 0 { // Check if the first heartbeat still hasn't arrived
			now := time.Now().UnixNano()
			if now-local < timeout {
				// Check if the worker has timed-out
				time.Sleep(time.Millisecond * 100)
				continue
			}
		}

		// Failed
		failedTimes++
		if failedTimes == 3 {
			// fmt.Printf("[Monitor] target process %d %d failed, local = %d\n", taskType, id, local)
			c.livenessMutex.Lock()
			*epoch++
			if taskType == tMap { // Put back to task queue
				c.mapTaskQueue.Enqueue(id)
			} else {
				c.reduceTaskQueue.Enqueue(id)
			}
			c.livenessMutex.Unlock()
			break
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.counterMutex.RLock()
	defer c.counterMutex.RUnlock()

	return c.nFinishedMapTask == c.nMaps && c.nFinishedReduceTask == c.nReduces
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMaps = len(files)
	c.nReduces = nReduce
	c.files = files

	c.nFinishedMapTask = 0
	c.mapTaskQueue = new(Queue)
	c.finishedMaps = make(map[int]bool)

	c.nFinishedReduceTask = 0
	c.reduceTaskQueue = new(Queue)
	c.finishedReduces = make(map[int]bool)

	c.taskEpoch = make([]int, c.nMaps+c.nReduces)
	c.taskLiveness = make([]int64, c.nMaps+c.nReduces)

	for i := 0; i < c.nMaps; i++ {
		c.mapTaskQueue.Enqueue(i)
	}
	for i := 0; i < c.nReduces; i++ {
		c.reduceTaskQueue.Enqueue(i)
	}

	c.server()
	return &c
}
