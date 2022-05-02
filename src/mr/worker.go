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

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// yang:
// for a worker who is assigned to a Task
// the worker apply the mapf & reducef to it
// the output data will be stored locally
type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *worker) acquireTask() Task {
	args := WhichWorker{WorkerID: w.id}
	reply := ReplyTask{}

	ok, err := call("Coordinator.AcquireATask", &args, &reply)

	if ok == false {
		if err.Error() == "unexpected EOF" {
			log.Printf("[woker-%d] exit normally.\n", w.id)
			os.Exit(1)
		} else {
			panic("Require task fail")
		}
	}
	//log.Printf("[worker-%d] get Task %v\n", w.id, reply.Task)

	return *reply.Task
}

// @method working used for a worker to get a Task from master and do the map-reduce
// the Task returned should be an Alive Task
func (w *worker) working() {
	for {

		//log.Printf("Try to acquire!")
		task := w.acquireTask()
		//log.Printf("aquired!")

		if !task.Alive {
			log.Printf("[worker-%d] get a dead Task,return\n", w.id)
			return
		}

		switch task.Stage {
		case Map:
			w.MAP(task)
		case Reduce:
			w.REDUCE(task)
		default:
			log.Fatalf("task stage unkown")
		}

		time.Sleep(scheduleTime)

	}
}

func (w *worker) MAP(task Task) {
	//log.Printf("[woker-%d] doing his [MAP-%d] job.\n", w.id, task.TaskID)
	filename := task.Filename

	// this is from the mr-seq.
	file, err := os.Open(filename)
	if err != nil {
		w.NOTICE(task, false, err)
		return
	}
	// store the content	log.Println()()("[woker-%d] doing his [MAP-%d] job.",w.id,task.TaskID)
	content, err := ioutil.ReadAll(file)
	if err != nil {
		w.NOTICE(task, false, err)
		return
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			w.NOTICE(task, false, err)
		}
	}(file)

	// MAP process
	kva := w.mapf(filename, string(content))
	// intermediate files should be NReduce bucks
	intermediate := make([][]KeyValue, task.NReduce)
	// process all the intermediate {k,v}
	// do the hashing process
	for _, kv := range kva {
		// all the {'key',1}
		// get a buck-idx for each string
		// as the paper mentioned
		buck_idx := ihash(kv.Key) % task.NReduce
		// add to the buck
		intermediate[buck_idx] = append(intermediate[buck_idx], kv)
	}

	// store the intermediate locally
	// each buck will be given a Filename
	// according to the lab-doc mr-X-Y where X is the Map Task number, and Y is the reduce Task number.
	// intermediate size NReduce
	// use tmp file to optimaze the process
	// once the master crash the file won't exist
	tmps := make(map[int]struct {
		*os.File
		*json.Encoder
	})
	// each intermediate --> a tmpfile with idx in tmps
	for idx := 0; idx < task.NReduce; idx++ {
		// generating tmp files
		tempFile, errtmp := ioutil.TempFile("", "inter-*")
		if errtmp != nil {
			w.NOTICE(task, false, errtmp)
			return
		}
		tmps[idx] = struct {
			*os.File
			*json.Encoder
		}{
			tempFile, json.NewEncoder(tempFile),
		}
	}

	for idx, inter := range intermediate {

		//// TaskID is a unique id for Task
		//// idx is the buck-idx for reduce
		//localfile := fmt.Sprintf("mr-%d-%d", task.TaskID, idx)
		//file, err := os.Create(localfile)
		//// create fail
		//if err != nil {
		//	w.NOTICE(task, false, err)
		//	return
		//}

		// the corresponding tmpfile
		file := tmps[idx].File
		// from the lab-doc
		enc := json.NewEncoder(file)
		// for the intermediate files
		// deal each buck
		// encode them using encoding/json
		for _, kv := range inter {
			err := enc.Encode(&kv)
			if err != nil {
				w.NOTICE(task, false, err)
			}
		}
	}

	for _, tmp := range tmps {
		tmp.Close()
	}

	for idx, tfile := range tmps {
		localfile := fmt.Sprintf("mr-%d-%d", task.TaskID, idx)
		if err := os.Rename(tfile.Name(), localfile); err != nil {
			w.NOTICE(task, false, err)
			log.Fatalf("MAP rename fail.")
		} else {
			log.Printf("[woker-%d] %v generated\n", w.id, localfile)
		}
	}

	// oops! everything ok
	w.NOTICE(task, true, nil)
	//log.Printf("[woker-%d] done his [MAP-%d] job.\n", w.id, task.TaskID)

}
func (w *worker) REDUCE(task Task) {
	//log.Printf("[woker-%d] doing his [REDUCE-%d] job.\n", w.id, task.TaskID)

	// map-Task : NMaps
	var intermediate = []KeyValue{}
	for idx := 0; idx < task.NMap; idx++ {
		// local file store the intermediate
		// here is reduce Task so this id = buck-idx
		// here idx = map-Task.id
		localfile := fmt.Sprintf("mr-%d-%d", idx, task.TaskID)
		file, err := os.Open(localfile)
		defer file.Close()
		if err != nil {
			log.Println("Sleep when no file")
			time.Sleep(time.Second * 8)
			w.NOTICE(task, false, err)
			return
		}
		// as the lab-doc
		dec := json.NewDecoder(file)
		// just store all intermediate
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sort the intermediate
	sort.Sort(ByKey(intermediate))

	tempFile, errtmp := ioutil.TempFile("", "out-*")
	defer tempFile.Close()
	if errtmp != nil {
		w.NOTICE(task, false, errtmp)
		return
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			// how many same key
			// {"hello",1}
			// {"hello",1}
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			// all the times from {key,value}
			// {"1","1","1",...}
			values = append(values, intermediate[k].Value)
		}
		// reduce Stage
		// output is a string of times of a special key
		output := w.reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			w.NOTICE(task, false, err)
		}
		// xxxxxxxx <key1>|j
		//          	  |i j
		// next key
		i = j
	}

	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	if terr := os.Rename(tempFile.Name(), oname); terr != nil {
		w.NOTICE(task, false, terr)
		log.Fatalf("Reduce rename fail")
	}
	log.Printf("[woker-%d] %v generated\n", w.id, oname)
	w.NOTICE(task, true, nil)
	//log.Printf("[woker-%d] done his [REDUCE-%d] job.\n", w.id, task.TaskID)

}
func (w *worker) NOTICE(task Task, finish bool, err error) {
	if err != nil {
		log.Printf("error when running :%v\n", err)
	}

	args := NoticeArgs{
		Done:     finish,
		TaskID:   task.TaskID,
		Stage:    task.Stage,
		WorkerID: w.id,
	}

	reply := ReplyArgs{}

	E, err := call("Coordinator.GetTaskReport", &args, &reply)
	if E != true {
		log.Printf("notice coordinator failure! info: %+v\n", args)
	}
}

func (w *worker) run(task Task) {
	log.Printf("[worker-%d] run a [Stage-%d] Task\n", w.id, task.Stage)
	switch task.Stage {
	case Map:
		w.MAP(task)
	case Reduce:
		w.REDUCE(task)
	default:
		log.Fatalf("No such Task Stage: %v", task.Stage)
	}
}

// @method register used for a worker to get registered in  master through rpc
// master should return an worker ID
func (w *worker) register() {
	// prepare args for rpc call
	args := &WorkerRegisterArgs{}
	reply := &WorkerRegisterID{}
	// send rpc to the master    Coordinator.RegisterWorker
	ok, err := call("Coordinator.RegisterWorker", &args, &reply)
	if ok != true {
		if err.Error() == "unexpected EOF" {
			log.Printf("Worker-%d normally exits\n", w.id)
			os.Exit(1)
		} else {
			log.Fatal("worker register Stage failure!")
		}
	}
	// the worker's id is replied
	w.id = reply.WorkerID
}

//
// main/mrworker.go calls this function.
//
// what the worker should Done will be here
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// yang:
	// get a worker and user-specific function
	var w = worker{}
	w.mapf = mapf
	w.reducef = reducef
	// register this worker in master
	w.register()
	// start working
	w.working()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.

	// the Coordinator is the type name
	// the Example is a method of Coordinator
	// args used for this method
	// reply used for accept return value
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	log.Printf("reply.Y %v \n\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)

	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true, nil
	}

	log.Println(err)
	return false, err
}
