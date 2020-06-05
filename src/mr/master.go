package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	mapTask          []string
	reduceTask       []ReduceTask
	nReduce, workers int
	doingMapTask     map[string]WorkStatus
	doingReduceTask  map[int]WorkStatus
	mux              sync.Mutex
}

type ReduceTask struct {
	taskId int
	fileList []string
}

type WorkStatus struct {
	id       int
	time     int64
	fileList []string
	file     []string
}
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) WorkerHandler(args *ExampleArgs, reply *ExampleReply) error {
	m.mux.Lock()
	if args.Id == 0 {
		m.workers++
		reply.Id = m.workers
	} else {
		reply.Id = args.Id
	}
	//fmt.Printf("master recieves id=%v , data = {%v}\n", reply.Id, args)

	if args.ReqType == "ask" {
		if len(m.mapTask) > 0 {
			reply.ReplyType = "map"
			reply.Filename = m.mapTask[0]
			reply.ReduceN = m.nReduce
			m.doingMapTask[reply.Filename] = WorkStatus{id:reply.Id, time:time.Now().Unix()}
			m.mapTask = m.mapTask[1:]
		} else if len(m.doingMapTask) > 0 {
			reply.ReplyType = "wait"
		} else if len(m.reduceTask) > 0 {
			reply.ReplyType = "reduce"
			rt := m.reduceTask[0]
			reply.TaskId = rt.taskId
			reply.ReduceList = rt.fileList
			m.doingReduceTask[rt.taskId] = WorkStatus{id:reply.Id, time: time.Now().Unix(), fileList: rt.fileList}
			m.reduceTask = m.reduceTask[1:]
		} else if len(m.doingReduceTask) > 0 {
			reply.ReplyType = "wait"
		} else {
			reply.ReplyType = "exit"
		}
	} else if args.ReqType == "mapRep" {
		v, ok := m.doingMapTask[args.FinishMap]
		if ok && v.id == args.Id {
			fileList := args.ReduceList
			for i, v := range fileList {
				m.reduceTask[i].fileList = append(m.reduceTask[i].fileList, v)
			}
			delete(m.doingMapTask, args.FinishMap)
		}
	} else if args.ReqType == "reduceRep" {
		v, ok :=  m.doingReduceTask[args.FinishReduce]
		if ok && v.id == args.Id {
			delete(m.doingReduceTask, args.FinishReduce)
		}
	}

	m.mux.Unlock()

	//fmt.Printf("master response id=%v , data = {%v}\n", reply.Id, reply)

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
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.CheckLong()
	return len(m.mapTask) == 0 && len(m.doingMapTask) == 0 && len(m.reduceTask) == 0 && len(m.doingReduceTask) == 0
}
func (m *Master) CheckLong() {
	m.mux.Lock()
	for k,v := range m.doingMapTask {
		if time.Now().Unix() - v.time > 10 {
			delete(m.doingMapTask, k)
			m.mapTask = append(m.mapTask, k)
		}
	}
	for k,v := range m.doingReduceTask {
		if time.Now().Unix() - v.time > 10 {
			delete(m.doingReduceTask, k)
			m.reduceTask = append(m.reduceTask, ReduceTask{taskId: k, fileList: v.fileList})
		}
	}
	m.mux.Unlock()
}
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	reduceTask := make([]ReduceTask, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTask[i] = ReduceTask{taskId: i}
	}

	m := Master{mapTask: files, reduceTask: reduceTask, nReduce: nReduce, doingMapTask: make(map[string]WorkStatus), doingReduceTask: make(map[int]WorkStatus)}
	m.server()
	return &m
}
