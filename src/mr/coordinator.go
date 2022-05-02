package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Task :the structure to assigned to each worker
type Task struct {
	Filename string
	TaskID   int
	NReduce  int
	NMap     int
	// map Stage or reduce Stage
	Stage taskStage
	Alive bool
}

// also the master should maintain the worker & Task table
type taskInfo struct {
	// as in the coordinator this TaskID is redundant
	// which Task
	// TaskID int

	// which worker deal this Task
	workerID int
	// what about this Task
	status taskStatus
	// related with max live time
	startTime time.Time
}

// Coordinator :the coordinator structure for worker to call
type Coordinator struct {
	// Your definitions here.

	files []string
	// used for hashing
	nReduce int
	nMap    int
	// Task-worker info
	// the index may represent Task
	// so the TaskID in taskInfo is redundant
	taskWorkerInfo []taskInfo
	// used for solving race condition
	masterLock sync.Mutex
	// used for generating WorkerID
	WIDseed int
	// Task-pool
	// use channel to buffer the tasks
	taskPool chan Task
	// all tasks finish
	done bool
	// MAP or REDUCE
	stage taskStage
	// reduce job cntR
	cntR int
	cntM int
}

// @method scheduler used for a providing master a Task-scheduler
// be careful with the race condition
func (c *Coordinator) scheduler() {
	// use defer!
	c.masterLock.Lock()
	defer c.masterLock.Unlock()
	//log.Println("schedule hit")
	// everything ok
	if c.done {
		return
	}
	var done bool = false
	for idx, job := range c.taskWorkerInfo {
		// state machine
		switch job.status {
		case tStatusReady:
			done = false
			c.taskPool <- c.requireTask(idx)
			// idle in queue
			// be careful about the trap
			// use job.status may not a good idea
			c.taskWorkerInfo[idx].status = tStatusIdle
		case tStatusInProgress:
			done = false
			if time.Now().Sub(job.startTime) > maxTaskLiveTime {
				log.Println("Timeout reallocate work")
				// the Task will be replaced in rpc call for a Task
				c.taskWorkerInfo[idx].status = tStatusIdle
				c.taskPool <- c.requireTask(idx)
			}
		case tStatusDone:
			done = true
		case tStatusIdle:
			done = false
		case tStatusErr:
			done = false
			// the Task will be replaced in rpc call for a Task
			c.taskWorkerInfo[idx].status = tStatusIdle
			c.taskPool <- c.requireTask(idx)
		default:
			panic("No such Task status")
		}

	}
	if done {
		if c.stage == Map {
			// all map finishes!
			log.Printf("map NUM:%d \n", c.cntM)
			if c.cntM == c.nMap {
				log.Println("All Map done. Now comes to reduce part")
				c.stage = Reduce
				c.taskWorkerInfo = make([]taskInfo, c.nReduce)
			}
			//log.Printf("New Info :%+v\n", c.taskWorkerInfo)
			//go c.scheduling()
		} else {
			// oops! reduce finish!
			//log.Println("EVERYTHING DONE!")
			//log.Printf("New Info :%+v\n", c.taskWorkerInfo)

			c.done = true
			//for _, job := range c.taskWorkerInfo {
			//	if job.status != tStatusDone {
			//		c.done = false
			//	}
			//}
			if c.cntR != c.nReduce {
				c.done = false
			}
			log.Printf("reduce NUM:%d DONE :%v\n", c.cntR, c.done)
		}
	}

}

// @method registerTask used for master to register a Task
func (c *Coordinator) registerTask(workerID int, task *Task) {
	c.masterLock.Lock()
	defer c.masterLock.Unlock()

	var info = taskInfo{
		workerID:  workerID,
		status:    tStatusInProgress,
		startTime: time.Now(),
	}
	// add to info array
	c.taskWorkerInfo[task.TaskID] = info

}

// return a Task
func (c *Coordinator) requireTask(taskID int) Task {
	// init a Task
	var task = Task{
		Filename: "",
		TaskID:   taskID,
		NReduce:  c.nReduce,
		NMap:     len(c.files),
		Stage:    c.stage,
		Alive:    true,
	}
	c.nMap = task.NMap
	//log.Printf("master:%v create task [TaskID:%d] [fileLen:%d] [taskNum:%d]\n", c, taskID, len(c.files), len(c.taskWorkerInfo))
	// dispatch a Map Task number
	if task.Stage == Map {
		task.Filename = c.files[taskID]
	}
	return task
}

// Your code here -- RPC handlers for the worker to call.

//========================================================================================================
//   RPC related implementation
//========================================================================================================

// @method AcquireATask used for worker to acquire a Task
// the Task will be registered and returned through rpc
// this is the interface interact with worker
func (c *Coordinator) AcquireATask(args *WhichWorker, reply *ReplyTask) error {
	// take a Task from taskPool
	//log.Println("channel Wait.")
	//log.Printf("poolsize %v", len(c.taskPool))
	var task = <-c.taskPool
	//log.Println("channel Get.")

	reply.Task = &task
	c.taskWorkerInfo[task.TaskID].status = tStatusInProgress

	if task.Stage == Reduce && c.stage != Reduce {
		panic("Task should be MAP Stage")
	}
	if task.Alive {
		c.registerTask(args.WorkerID, reply.Task)
	}

	log.Printf("[worker-%d] get [Task-%d] %+v\n", args.WorkerID, task.TaskID, task)
	return nil
}

// @method RegisterWorker used for a worker to be registered
func (c *Coordinator) RegisterWorker(args *WorkerRegisterArgs, reply *WorkerRegisterID) error {
	c.masterLock.Lock()
	defer c.masterLock.Unlock()
	c.WIDseed += 1
	reply.WorkerID = c.WIDseed
	return nil
}

// @method GetTaskReport used for worker to notice the master the Task status
func (c *Coordinator) GetTaskReport(args *NoticeArgs, reply *ReplyArgs) error {
	c.masterLock.Lock()
	defer c.masterLock.Unlock()

	log.Printf("Report from [worker-%d] : %+v \n", args.WorkerID, args)

	// make sure for the update of Info
	if c.stage != args.Stage || args.WorkerID != c.taskWorkerInfo[args.TaskID].workerID {
		return nil
	}

	if args.Done {
		c.taskWorkerInfo[args.TaskID].status = tStatusDone
	} else {
		c.taskWorkerInfo[args.TaskID].status = tStatusErr
	}

	if c.stage == Reduce {
		c.cntR++
	}
	if args.Stage == Map {
		c.cntM++
	}
	// do scheduling
	go c.scheduler()

	return nil
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	// Your code here.
	c.masterLock.Lock()
	defer c.masterLock.Unlock()
	return c.done
}

func (c *Coordinator) scheduling() {
	for !c.done {
		go c.scheduler()
		time.Sleep(scheduleTime)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// init coordinator

	// all the local txt files
	c.files = files
	// user specific
	c.nReduce = nReduce
	c.masterLock = sync.Mutex{}
	// WorkerID start from 0
	c.WIDseed = 0
	var size = func(a, b int) int {
		if a > b {
			return a
		} else {
			return b
		}
	}(len(files), nReduce)
	// enough size for the channel
	// how many tasks -- max(len(files), NReduce)
	c.taskPool = make(chan Task, size)
	// deal the Task Stage & each file->Task->worker
	// init map jobs
	c.stage = Map
	// each file ---> taskInfo
	c.taskWorkerInfo = make([]taskInfo, len(c.files))
	// maybe deal with all the Task
	// scheduler needed
	go c.scheduling()

	log.Printf("Master created : %+v\n", c)
	log.Println("Coordinator init finish!")
	// Your code here.
	// start to providing service for workers
	c.server()
	return &c
}
