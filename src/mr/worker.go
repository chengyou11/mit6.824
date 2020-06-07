package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	id := 0
	for true {
		reply := askForCall("ask", id, 0, nil, "")
		id = reply.Id
		if reply.ReplyType == "map" {
			mapList := mapFunc(mapf, reply.Filename, reply.ReduceN)
			askForCall("mapRep", id, 0, mapList, reply.Filename)
		} else if reply.ReplyType == "reduce" {
			reduceFunc(reducef, reply.ReduceList, reply.TaskId)
			askForCall("reduceRep", id, reply.TaskId, nil, "")
		} else if reply.ReplyType == "wait" {
			time.Sleep(time.Second)
		} else {
			os.Exit(0)
		}
	}
}

func mapFunc(mapf func(string, string) []KeyValue, filename string, nReduce int) []string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("cound not open file")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("cound not open file")
	}
	err = file.Close()
	if err != nil {
		log.Fatal("close file error", err)
	}
	kvm := mapf(filename, string(content))
	contents := make([][]KeyValue, nReduce)

	for _, kv := range kvm {
		n := ihash(kv.Key) % nReduce
		contents[n] = append(contents[n], kv)
	}

	var fileList []string
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%v-%v", filename[3:], i)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatal("fail to create file ", err)
		}
		enc := json.NewEncoder(ofile)

		for _, kv := range contents[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("fail to save map result to file", err)
			}
		}
		err = ofile.Close()
		if err != nil {
			log.Fatal("close file error", err)
		}
		fileList = append(fileList, oname)
	}

	return fileList
}
func reduceFunc(reducef func(string, []string) string, fileList []string, taskId int) {
	var intermediate []KeyValue
	oname := fmt.Sprintf("mr-out-%v", taskId)
	f, _ := os.Create(oname)

	for _, v := range fileList {
		file, err := os.Open(v)
		if err != nil {
			log.Fatal("cound not open file")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		err = file.Close()
		if err != nil {
			log.Fatal("close file error", err)
		}
	}

	i := 0
	sort.Sort(ByKey(intermediate))

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
		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	f.Close()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func askForCall(req string, id int, FinishReduce int, ReduceList []string, finishMap string) ExampleReply {
	args := ExampleArgs{ReqType: req, Id: id, FinishMap: finishMap, FinishReduce: FinishReduce, ReduceList: ReduceList}
	reply := ExampleReply{}

	call("Master.WorkerHandler", &args, &reply)

	return reply
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
