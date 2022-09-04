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
	"strconv"
	"strings"
)

//
// KeyValue is ...
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type Resource struct {
	NMap    int
	NReduce int
}

type MapRequestArgs struct {
}

type MapRequestReply struct {
	ProcessID int
	FileID    int
	FileName  string
	OK        bool
}

type MapResponseArgs struct {
	ProcessID int
	FileID    int
	FileNames []string
}

type MapResponseReply struct {
	OK bool
}

func mapWorker(nReduce int, processID int, fileName string, fileID int, mapf func(string, string) []KeyValue) {
	//fmt.Printf("map: processing file: %s\n", fileName)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v, err %s", fileName, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v err %s", fileName, err)
	}
	file.Close()
	kva := mapf(fileName, string(content))

	//fmt.Printf("mapWorker: get mapf result: %v\n", kva)

	args := MapResponseArgs{
		ProcessID: processID,
		FileID:    fileID,
	}
	args.FileNames = make([]string, nReduce)
	intermediate := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		// 每个 reduce 负责多个 key，reduce 之间的 key 不重叠
		kh := ihash(kv.Key) % nReduce
		intermediate[kh] = append(intermediate[kh], kv)
	}
	for i := 0; i < nReduce; i++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("cannot create tmp file, err %s", err)
		}
		args.FileNames[i] = tmpFile.Name()
		enc := json.NewEncoder(tmpFile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v, err %s", fileName, err)
			}
		}
		tmpFile.Close()
	}

	reply := MapResponseReply{}
	call("Master.MapResponse", &args, &reply)
}

type ReduceRequestArgs struct {
}

type ReduceRequestReply struct {
	ProcessID int
	ReduceID  int
	OK        bool
}

type ReduceResponseArgs struct {
	ProcessID int
	ReduceID  int
	FileName  string
}

type ReduceResponseReply struct {
	OK bool
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func reduceWorker(processID int, reduceID int, reducef func(string, []string) string) {
	dir := "./"
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatalf("cannot read dir %s, err %s", dir, err)
	}
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("cannot create tmp file, err %s", err)
	}

	var intermediate []KeyValue

	for _, file := range files {
		fileName := file.Name()
		if !strings.HasPrefix(fileName, "mr-intermediate-") {
			continue
		}

		x, err := strconv.Atoi(fileName[len(fileName)-1:])
		if err != nil {
			log.Fatalf("cannot atoi %v, err %s", fileName[len(fileName)-1:], err)
		}
		if x != reduceID {
			continue
		}

		file, err := os.Open(dir + fileName)
		if err != nil {
			log.Fatalf("cannot open %v, err %s", fileName, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpFile.Close()

	args := ReduceResponseArgs{
		ProcessID: processID,
		ReduceID:  reduceID,
		FileName:  tmpFile.Name(),
	}
	reply := ReduceRequestReply{}
	call("Master.ReduceResponse", &args, &reply)
}

//
// Worker is ...
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	res := Resource{}
	call("Master.GetResources", &res, &res)

	//fmt.Printf("Worker: get resources: NMap: %d, NReduce: %d\n", res.NMap, res.NReduce)

	// map work
	for {
		args := MapRequestArgs{}
		reply := MapRequestReply{}
		call("Master.MapRequest", &args, &reply)

		// 全部执行完了，意味着 chan 关闭，即 m.mapNum == m.fileNum，因此可以直接进行 reduce
		if !reply.OK {
			break
		}

		go mapWorker(res.NReduce, reply.ProcessID, reply.FileName, reply.FileID, mapf)
	}

	// reduce work
	for {
		args := ReduceRequestArgs{}
		reply := ReduceRequestReply{}
		call("Master.ReduceRequest", &args, &reply)

		if !reply.OK {
			break
		}

		go reduceWorker(reply.ProcessID, reply.ReduceID, reducef)
	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func callExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
