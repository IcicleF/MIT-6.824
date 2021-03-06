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
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Mapper
func Map(mapf func(string, string) []KeyValue, nReduces int, filename string, id int) {
	// fmt.Printf("[Worker.Map][%d] nReduces=%d, map %v\n", id, nReduces, filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))

	subsets := make([][]KeyValue, nReduces)
	for _, kv := range kva {
		sid := ihash(kv.Key) % nReduces
		subsets[sid] = append(subsets[sid], kv)
	}

	for i := 0; i < nReduces; i++ {
		outf := fmt.Sprintf("mr-int-%d-%d", id, i)
		if err != nil {
			log.Fatalf("cannot create temp file")
		}

		serialized, _ := json.Marshal(subsets[i])
		if err := ioutil.WriteFile(outf, serialized, 0644); err != nil {
			log.Fatalf("cannot write to temp file")
		}
	}
}

// Reducer
func Reduce(reducef func(string, []string) string, nMaps int, id int) {
	kva := []KeyValue{}
	for i := 0; i < nMaps; i++ {
		inf := fmt.Sprintf("mr-int-%d-%d", i, id)
		file, err := os.Open(inf)
		if err != nil {
			log.Fatalf("cannot open file %v", inf)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read file %v", inf)
		}
		file.Close()

		subset := []KeyValue{}
		json.Unmarshal(content, &subset)
		kva = append(kva, subset...)
	}

	sort.Sort(ByKey(kva))

	outf := fmt.Sprintf("mr-out-%d", id)
	file, err := ioutil.TempFile(".", "mr-out-tmp-")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	tmpoutf := file.Name()

	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		i = j
	}

	file.Close()

	// Atomically move temp file to outfile
	os.Rename(tmpoutf, outf)
}

// Remove temp files
// func ReduceCleanup(nMaps int, id int) {
// 	for i := 0; i < nMaps; i++ {
// 		inf := fmt.Sprintf("mr-int-%d-%d", i, id)
// 		os.Remove(inf)
// 	}
// }

type Indicator struct {
	mutex sync.Mutex
	fin   bool
}

func (i *Indicator) GetInd() bool {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	return i.fin
}

func (i *Indicator) SetInd(x bool) {
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.fin = x
}

// Heartbeat
func Heartbeat(taskType int, id int, i *Indicator) {
	request := GeneralCarrier{}
	reply := GeneralCarrier{}

	for {
		if i.GetInd() {
			break
		}

		request.Kind = taskType
		request.Id = id
		call("Coordinator.Heartbeat", &request, &reply)
		if reply.Kind != MsgOK {
			log.Fatalf("heartbeat failed")
		}
		time.Sleep(time.Millisecond * 50)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	request := GeneralCarrier{}
	reply := GeneralCarrier{}
	call("Coordinator.FetchTask", &request, &reply)

	for reply.Kind != MsgNoTask {
		switch reply.Kind {
		case MsgMapTask:
			id := reply.Id
			nReduces := reply.NReduces
			ind := Indicator{fin: false}
			// fmt.Printf("[Worker.%d] received map task %d\n", os.Getpid(), id)

			go Heartbeat(tMap, id, &ind)
			Map(mapf, nReduces, reply.Msg, id%reply.NMaps)
			ind.SetInd(true)

			request.Kind = MsgMapFinished
			request.Id = id
			// fmt.Printf("[Worker] map task %d finished\n", id)
			call("Coordinator.Report", &request, &reply)
			// fmt.Printf("[Worker.%d] map task %d reported\n", os.Getpid(), id)
		case MsgReduceTask:
			id := reply.Id
			nMaps := reply.NMaps
			ind := Indicator{fin: false}
			// fmt.Printf("[Worker.%d] received reduce task %d\n", os.Getpid(), id)

			go Heartbeat(tReduce, id, &ind)
			Reduce(reducef, nMaps, id%reply.NReduces)
			ind.SetInd(true)

			request.Kind = MsgReduceFinished
			request.Id = id
			// fmt.Printf("[Worker] reduce task %d finished\n", id)
			call("Coordinator.Report", &request, &reply)
			// fmt.Printf("[Worker.%d] reduce task %d reported\n", os.Getpid(), id)
			// ReduceCleanup(nMaps, id)
		case MsgWaitTask:
			time.Sleep(time.Millisecond * 100)
		default:
			log.Fatalf("unexpected task type %d", reply.Kind)
		}

		request = GeneralCarrier{}
		reply = GeneralCarrier{}
		call("Coordinator.FetchTask", &request, &reply)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
