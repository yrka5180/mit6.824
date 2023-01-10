package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	for {
		args := TaskArgs{}
		reply := TaskReply{}
		// 如果rpc报错
		ok := call("Master.AcquireTask", &args, &reply)
		if !ok {
			break
		}

		// 正常获取任务
		// 任务可能是 map 或者 reduce
		switch reply.TaskType {
		case MapTask:
			// fmt.Println("get map task, file lists:", reply.FileList)
			// 将kv根据
			fList := doMap(mapf, reply)
			// 完成后通知master
			notice(fList, reply.TaskType, reply.TaskID)
			// after processing continue find another task, until all task done
		case ReduceTask:
			// fmt.Println("get reduce task", reply.FileList)
			doReduce(reducef, reply)
			// 完成后通知master
			notice(nil, reply.TaskType, reply.TaskID)
		}
	}
}

// notice 当map task完成时，通知master中间文件地址
func notice(addrs []string, jobType, taskID int) {
	args := NoticeArgs{
		TaskType: jobType,
		TaskID:   taskID,
		MapAddrs: addrs,
	}
	call("Master.TaskDone", &args, nil)
}

// doMap 执行map task
// 返回值是中间文件路径
func doMap(mapf func(string, string) []KeyValue, t TaskReply) []string {
	// 将文件根绝单词划分
	intermediate := []KeyValue{}
	fList := make([]string, t.NReduce)
	for _, filename := range t.FileList {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
			continue
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
			continue
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	// 根据key 分片
	// nReduce个文件
	memKV := make([][]KeyValue, t.NReduce)
	for i := range memKV {
		memKV[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		r := ihash(kv.Key) % t.NReduce
		// 将kv放入对应的内存r中
		memKV[r] = append(memKV[r], kv)
	}
	// write into file
	for i, kvs := range memKV {

		fName := fmt.Sprintf("mr-%d-%d", t.TaskID, i)
		fList[i] = filepath.Join("mr_out", fName)
		// kvs是所有要落盘到第i个reduce task的集合
		f, err := os.Create(fList[i])
		if err != nil {
			log.Println(err)
			continue
		}
		enc := json.NewEncoder(f)
		if err != nil {
			log.Println(err)
			continue
		}
		err = enc.Encode(&kvs)
		if err != nil {
			log.Println(err)
			continue
		}
	}
	return fList
}

func doReduce(reducef func(string, []string) string, t TaskReply) {
	// 执行reduce task
	intermediate := []KeyValue{}
	for _, f := range t.FileList {
		fmt.Println()
		fd, err := os.Open(f)
		if err != nil {
			log.Println(err)
			continue
		}
		// 解析kv pairs
		kvs := []KeyValue{}
		dec := json.NewDecoder(fd)
		err = dec.Decode(&kvs)
		if err != nil {
			log.Println(err)
			continue
		}
		// 将
		intermediate = append(intermediate, kvs...)
	}
	// 排序
	sort.Sort(ByKey(intermediate))

	// 统计
	oname := fmt.Sprintf("mr-out-%v", t.TaskID)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

}

//
// example function to show how to make an RPC call to the master.
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
