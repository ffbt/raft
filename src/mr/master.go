package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type fileState struct {
	fileID    int
	name      string
	done      bool
	processID int
	mu        sync.Mutex
}

type reduceState struct {
	reduceID  int
	done      bool
	processID int
	mu        sync.Mutex
}

type Master struct {
	// Your definitions here.
	nReduce, nMap     int
	mapNum, reduceNum int32
	fileNum           int32
	done              bool
	fileStates        []fileState
	reduceStates      []reduceState
	mapCh, reduceCh   chan int
	mapID, reduceID   int32 // 用来分配 id
}

// Your code here -- RPC handlers for the worker to call.

const DELAY_TIME = 10

// GetResources is ...
func (m *Master) GetResources(args *Resource, reply *Resource) error {
	//fmt.Printf("GetResources: m.nMap = %d, m.nReduce = %d\n", m.nMap, m.nReduce)
	reply.NMap = m.nMap
	reply.NReduce = m.nReduce
	return nil
}

// consumer
func (m *Master) MapRequest(args *MapRequestArgs, reply *MapRequestReply) error {
	//fmt.Printf("MapRequest: request id : %d\n", args.ProcessID)
	reply.OK = false // 若下面 chan 关闭，则返回 false
	for fileID := range m.mapCh {
		m.fileStates[fileID].mu.Lock()
		if m.fileStates[fileID].done {
			m.fileStates[fileID].mu.Unlock()
			continue
		}

		processID := atomic.AddInt32(&m.mapID, 1)
		m.fileStates[fileID].processID = int(processID)
		m.fileStates[fileID].mu.Unlock()

		reply.ProcessID = int(processID)
		reply.FileID = fileID
		reply.FileName = m.fileStates[fileID].name
		reply.OK = true

		// producer
		go func(fileID int) {
			time.Sleep(time.Duration(DELAY_TIME) * time.Second)
			// 不必进入临界区，读出以后会进入临界区中检查
			if !m.fileStates[fileID].done {
				m.mapCh <- fileID
			}
		}(fileID)

		break
	}

	return nil
}

func (m *Master) MapResponse(args *MapResponseArgs, reply *MapResponseReply) error {
	m.fileStates[args.FileID].mu.Lock()
	if args.ProcessID == m.fileStates[args.FileID].processID {
		m.fileStates[args.FileID].done = true
		m.fileStates[args.FileID].mu.Unlock()

		for i, oldFileName := range args.FileNames {
			newFileName := "mr-intermediate-" + strconv.Itoa(args.ProcessID) + "-" + strconv.Itoa(i)
			err := os.Rename(oldFileName, newFileName)
			if err != nil {
				log.Fatalf("cannot rename %v to %v, err %s", oldFileName, newFileName, err)
			}
		}

		atomic.AddInt32(&m.mapNum, 1)
		if m.mapNum == m.fileNum {
			close(m.mapCh)
		}
		reply.OK = true
	} else {
		m.fileStates[args.FileID].mu.Unlock()
		for _, oldFileName := range args.FileNames {
			err := os.Remove(oldFileName)
			if err != nil {
				log.Fatalf("cannot remove %v, err %s", oldFileName, err)
			}
		}
		reply.OK = false
	}

	return nil
}

func (m *Master) ReduceRequest(args *ReduceRequestArgs, reply *ReduceRequestReply) error {
	reply.OK = false
	for reduceID := range m.reduceCh {
		m.reduceStates[reduceID].mu.Lock()
		if m.reduceStates[reduceID].done {
			m.reduceStates[reduceID].mu.Unlock()
			continue
		}

		processID := atomic.AddInt32(&m.reduceID, 1)
		m.reduceStates[reduceID].processID = int(processID)
		m.reduceStates[reduceID].mu.Unlock()

		reply.ProcessID = int(processID)
		reply.ReduceID = reduceID
		reply.OK = true

		go func(reduceID int) {
			time.Sleep(time.Duration(DELAY_TIME) * time.Second)
			if !m.reduceStates[reduceID].done {
				m.reduceCh <- reduceID
			}
		}(reduceID)

		break
	}
	return nil
}

func (m *Master) ReduceResponse(args *ReduceResponseArgs, reply *ReduceResponseReply) error {
	m.reduceStates[args.ReduceID].mu.Lock()
	if args.ProcessID == m.reduceStates[args.ReduceID].processID {
		m.reduceStates[args.ReduceID].done = true
		m.reduceStates[args.ReduceID].mu.Unlock()
		newFileName := "mr-out-" + strconv.Itoa(args.ReduceID)
		err := os.Rename(args.FileName, newFileName)
		if err != nil {
			log.Fatalf("cannot rename %v to %v, err %s", args.FileName, newFileName, err)
		}
		atomic.AddInt32(&m.reduceNum, 1)
		if m.reduceNum == int32(m.nReduce) {
			close(m.reduceCh)

			dir := "./"
			files, err := ioutil.ReadDir(dir)
			if err != nil {
				log.Fatalf("cannot read dir %s, err %s", dir, err)
			}

			for _, file := range files {
				if strings.HasPrefix(file.Name(), "mr-intermediate-") {
					err := os.Remove(dir + file.Name())
					if err != nil {
						log.Fatalf("cannot remove %v, err %s", file.Name(), err)
					}
				}
			}
		}
		reply.OK = true
	} else {
		m.reduceStates[args.ReduceID].mu.Unlock()
		err := os.Remove(args.FileName)
		if err != nil {
			log.Fatalf("cannot remove %v, err %s", args.FileName, err)
		}
		reply.OK = false
	}
	return nil
}

//
// Example is an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// Done is ...
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = m.reduceNum == int32(m.nReduce)

	return ret
}

//
// MakeMaster : create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.nReduce, m.nMap = nReduce, nReduce
	m.fileNum = int32(len(files))
	m.mapNum, m.reduceNum = 0, 0
	m.mapID, m.reduceID = -1, -1
	m.fileStates = make([]fileState, m.fileNum)
	m.reduceStates = make([]reduceState, nReduce)
	m.mapCh = make(chan int, m.fileNum) // 缓冲区大小必须不能小于 m.fileNum，否则在执行 m.server() 之前将会阻塞
	m.reduceCh = make(chan int, nReduce)

	for i, file := range files {
		m.fileStates[i].fileID = i
		m.fileStates[i].name = file
		m.fileStates[i].done = false
		m.fileStates[i].processID = -1
		m.mapCh <- i
	}

	for i := 0; i < nReduce; i++ {
		m.reduceStates[i].reduceID = i
		m.reduceStates[i].done = false
		m.reduceCh <- i
	}

	m.server()
	return &m
}
