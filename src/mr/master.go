package mr

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	// channel
	TaskMapCh     chan *TaskReply // map task channel
	TaskReduceCh  chan *TaskReply // reduce task channel
	TaskMapNum    int             // map任务数
	TaskReduceNum int             // reduce任务数
	TaskID        int

	MapTaskCnt    int // map task完成数量
	ReduceTaskCnt int // reduce task完成数量

	MapDistributeDone bool // map 任务是否全部生成完成
	ReduceDistribDone bool // reduce 任务分发完成

	MapDone bool // map 任务完成

	ReduceTaskList [][]string // reduce task的中间文件地址列表

	doneCh chan struct{} // tell if master finished
}

const (
	MapTask    = 1
	ReduceTask = 2
)

var (
	ErrMapNotDone = errors.New("map task still in process")
	ErrNoTaskLeft = errors.New("no task left")
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// TaskDone 接收worker通知map任务结束通知
func (m *Master) TaskDone(args *NoticeArgs, reply *NoticeReply) error {
	switch args.TaskType {
	case MapTask:
		// 每收集一批r个中间文件, 记录下来
		addrs := args.MapAddrs
		for _, addr := range addrs {
			// 文件命名 mr-x-y x是map任务id，y是reduce任务id
			ss := strings.Split(addr, "-")
			r, _ := strconv.Atoi(ss[2])
			// 将中间文件地址放入对应的reduce列表
			m.ReduceTaskList[r] = append(m.ReduceTaskList[r], addr)
		}
		m.MapTaskCnt++
		if m.MapDistributeDone && m.MapTaskCnt == m.TaskID {
			// 如果map任务全部分发完成，并且map task完成数量 和 任务数量相同，表示任务map task全部分发完成
			m.MapDone = true
			// 生成 reduce task
			go m.makeReduceTask()
		}
	case ReduceTask:
		m.ReduceTaskCnt++
		//
		if m.ReduceDistribDone && m.ReduceTaskCnt == m.TaskReduceNum {
			m.doneCh <- struct{}{}
		}
	}
	return nil
}

// AcquireTask 任务请求
func (m *Master) AcquireTask(args *TaskArgs, reply *TaskReply) error {
	// 先获取map task
	if job, ok := <-m.TaskMapCh; ok {
		// 从map ch中拿取task
		*reply = *job
		fmt.Println("get map from channel", job)
		return nil
	}
	// 如果map task还没有结束，需要等待
	if !m.MapDone {
		return ErrMapNotDone
	}
	// 获取 reduce task
	if job, ok := <-m.TaskReduceCh; ok {
		*reply = *job
		fmt.Println("get reduce from channel", job)
		return nil
	}
	// 如果都无法获取，则表示任务任务全部分发结束
	return ErrNoTaskLeft
}

// makeMapTask 生成map task
func (m *Master) makeMapTask(files []string) {
	// 每个文件作为一个task发个一个worker
	for _, f := range files {
		job := TaskReply{}
		job.TaskType = MapTask
		job.FileList = []string{f}
		job.TaskID = m.getTaskID()
		job.NReduce = m.TaskReduceNum
		fmt.Println("put file", f, "into channel")
		// 将job放入jobCh
		m.TaskMapCh <- &job
	}
	// 关闭task map channel
	close(m.TaskMapCh)
	m.MapDistributeDone = true
}

// makeReduceTask 生成reduce task
func (m *Master) makeReduceTask() {
	// 根据r个文件
	for r, addrs := range m.ReduceTaskList {
		// r表示reduce task id
		// addrs是r对应的中间文件地址
		job := TaskReply{
			TaskType: ReduceTask,
			FileList: addrs,
			TaskID:   r,
			NReduce:  m.TaskReduceNum,
		}
		m.TaskReduceCh <- &job
	}
	// 关闭reduce channel
	close(m.TaskReduceCh)
	m.ReduceDistribDone = true
}

// getTaskID 生成任务id
func (m *Master) getTaskID() int {
	id := m.TaskID
	m.TaskID++
	return id
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
	ret := false

	// Your code here.
	// all task done
	<-m.doneCh
	ret = true
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.doneCh = make(chan struct{})
	// Your code here.
	// 几个map worker
	n := len(files)
	// 初始化map channel 和 reduce channel
	m.TaskMapCh = make(chan *TaskReply, n)
	m.TaskReduceCh = make(chan *TaskReply, nReduce)
	m.TaskReduceNum = nReduce
	m.TaskID = 0 // 从0开始
	m.ReduceTaskList = make([][]string, nReduce)
	for i := range m.ReduceTaskList {
		m.ReduceTaskList[i] = make([]string, 0)
	}

	// 执行生成map task
	go m.makeMapTask(files)
	// 分配任务 将文件分割给nReduce
	m.server()
	return &m
}
