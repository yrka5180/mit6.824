package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// TaskArgs 任务RPC参数
type TaskArgs struct {
}

// TaskReply 任务RPC响应
type TaskReply struct {
	TaskType int      `json:"task_type"` // 任务类型, map 1 reduce 2
	FileList []string `json:"file_list"` // 任务文件列表
	TaskID   int      `json:"task_id"`   // task id
	NReduce  int      `json:"n_reduce"`  // nReduce
}

// NoticeArgs 通知RPC参数, worker通知master,
type NoticeArgs struct {
	TaskType      int      `json:"task_type"`       // 任务类型
	TaskID        int      `json:"task_id"`         // 当前任务id
	InterFileList []string `json:"inter_file_list"` // 当前任务生成的中间文件地址
}

type NoticeReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
