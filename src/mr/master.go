package mr

import (
	"errors"
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
	// channel
	TaskMapCh     chan *TaskReply // map task channel
	TaskReduceCh  chan *TaskReply // reduce task channel
	TaskMapNum    int             // map任务数
	TaskReduceNum int             // reduce任务数
	TaskID        int

	MapDone bool // map 任务完成

	InterFileList [][]string // 中间文件地址列表, 根据

	doneCh chan struct{} // tell if master finished

	mu sync.Mutex // 任务锁，防止worker竞争任务

	phase int // 任务执行状态 1: map procedure 2: in reduce procedure

	metaData map[int]*metaInfo // 记录任务元数据，任务id，是否完成
}

type metaInfo struct {
	task  *Task // 任务信息
	state int   // 任务执行状态
}

type Task struct {
	TaskReply
}

// task类型
const (
	MapTask = iota + 1
	ReduceTask
	WaitingTask // 任务等待中间状态
	ExitedTask  // 标记任务完成，需要退出
)

// task状态
const (
	StateProcessing = iota + 1
	StateDone
)

// 流程执行进度
const (
	PhaseMap = iota + 1
	PhaseReduce
	PhaseAllDone
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
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args.TaskType {
	case MapTask:
		// 标记任务执行完成，修改对应map worker状态
		// 更新meta信息
		if meta, ok := m.metaData[args.TaskID]; ok && meta.state == StateProcessing {
			meta.state = StateDone
			// log.Printf("map task %v done \n", args.TaskID)
			// 更新中间文件列表
			for i, f := range args.InterFileList {
				m.InterFileList[i] = append(m.InterFileList[i], f)
			}
		}
	case ReduceTask:
		// 标记任务执行完成
		// 更新meta信息
		if meta, ok := m.metaData[args.TaskID]; ok && meta.state == StateProcessing {
			meta.state = StateDone
			// log.Printf("reduce task %v done \n", args.TaskID)
		}
	default:
		log.Fatalf("unkonwn task type %v", args.TaskType)
	}
	return nil
}

// AcquireTask 任务请求
func (m *Master) AcquireTask(args *TaskArgs, reply *TaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 根据master当前执行流程判断
	switch m.phase {
	case PhaseMap:
		// 先获取map task
		if job, ok := <-m.TaskMapCh; ok {
			// 从map ch中拿取task
			*reply = *job
			// fmt.Println("get map from channel", job)
			return nil
		}
		// map任务分发完成，但是没有进入reduce任务，等待
		reply.TaskType = WaitingTask
		// 判断当前mapreduce进度
		if m.isTaskAllDone(MapTask) {
			// 进入reduce procedure
			m.phase = PhaseReduce
			// 生成reduce task
			m.makeReduceTask()
		}
	case PhaseReduce:
		// TODO
		// 先获取map task
		if job, ok := <-m.TaskReduceCh; ok {
			// 从reduce ch中拿取task
			*reply = *job
			return nil
		}
		// reduce任务分发完成，先等待
		reply.TaskType = WaitingTask
		// 判断当前mapreduce进度
		if m.isTaskAllDone(ReduceTask) {
			// 进入all done phase
			// fmt.Println("change phase to all done")
			m.phase = PhaseAllDone
			m.doneCh <- struct{}{} // 可以退出master了
		}
	case PhaseAllDone:
		reply.TaskType = ExitedTask
	default:
		reply.TaskType = WaitingTask
	}
	// 如果都无法获取，则表示任务任务全部分发结束
	return nil
}

// isTaskAllDone 判断map task是否全部完成
func (m *Master) isTaskAllDone(typ int) bool {
	// 根据meta信息判断
	for _, meta := range m.metaData {
		if meta.task.TaskType == typ && meta.state == StateProcessing {
			// 如果还有未完成的任务，那么就是没有全部执行完成
			return false
		}
	}
	return true
}

// makeMapTask 生成map task
func (m *Master) makeMapTask(files []string) {
	// 每个文件作为一个task发个一个worker
	for _, f := range files {
		id := m.getTaskID()
		job := TaskReply{
			TaskType: MapTask,
			FileList: []string{f},
			TaskID:   id,
			NReduce:  m.TaskReduceNum,
		}
		// fmt.Printf("generate map task %+v\n", job)
		// 记录当前map任务元信息
		m.metaData[id] = &metaInfo{
			&Task{job},      // 任务主体
			StateProcessing, // 任务状态
		}
		// 将job放入jobCh
		m.TaskMapCh <- &job
	}
	// 关闭task map channel
	close(m.TaskMapCh)
}

// makeReduceTask 生成reduce task
func (m *Master) makeReduceTask() {
	// 根据r个文件
	for i := 0; i < m.TaskReduceNum; i++ {
		id := m.getTaskID()
		job := TaskReply{
			TaskID:   id,
			TaskType: ReduceTask,
			FileList: m.InterFileList[i], // 需要被处理的中间文件列表
		}
		// fmt.Printf("generate reduce task %+v\n", job)
		// 记录当前任务元信息
		m.metaData[id] = &metaInfo{
			&Task{job},      // 任务主体
			StateProcessing, // 任务状态
		}
		m.TaskReduceCh <- &job
	}
	// 关闭reduce channel
	close(m.TaskReduceCh)
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
	// Your code here.
	// all task done
	<-m.doneCh
	// 等所有worker都退出
	time.Sleep(time.Second * 2)
	return true
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
	m.InterFileList = make([][]string, nReduce)
	for i := range m.InterFileList {
		m.InterFileList[i] = make([]string, 0)
	}
	m.metaData = make(map[int]*metaInfo)
	m.phase = PhaseMap
	// 执行生成map task
	m.makeMapTask(files)
	// 分配任务 将文件分割给nReduce
	m.server()
	return &m
}
