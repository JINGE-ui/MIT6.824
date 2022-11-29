package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
//import "fmt"
import "io/ioutil"
import "sync"
import "strings"
import "strconv"

var mu sync.Mutex

//发送给worker的任务
//中间文件命名 mr-X-Y X是map任务号，y是reduce任务号
type Job struct{
	JobType JobType    //任务类型
	InputFile []string      //文件名，这里小文件无需切片
	JobId int    //X
	ReducerNum int      //y是经过hash后的reduce id, y用来标识哪些文件汇入同一个reduce
}
//所有任务的元数据管理
type JobMetaHolder struct {
	MetaMap map[int]*JobMetaInfo   //每一个jobid有唯一的元数据
}

type JobMetaInfo struct {
	condition JobCondition      //waiting/working/done  
	StartTime time.Time     //开始执行时间
	JobPtr    *Job
}

type Coordinator struct {
	// Your definitions here.
	JobChannelMap        chan *Job    //消息队列存放map任务
	JobChannelReduce     chan *Job     
	ReducerNum           int          //总共reduce的数目
	MapNum               int          //总共map的数目
	CoordinatorCondition Condition    //mapreduce所处阶段，map/reduce/alldone
	uniqueJobId          int              //用于元数据管理
	jobMetaHolder        JobMetaHolder       //元数据管理
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	//fmt.Println("+++++++++++++++++++++++++++++++++++++++++++")
	return c.CoordinatorCondition == AllDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobChannelMap:    make(chan *Job, len(files)),
		JobChannelReduce: make(chan *Job, nReduce),
		jobMetaHolder: JobMetaHolder{
			MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce),
		},
		CoordinatorCondition: MapPhase,
		ReducerNum:           nReduce,
		MapNum:               len(files),
		uniqueJobId:          0,
	}

	// Your code here.
	c.makeMapJobs(files)

	c.server()
	//go c.CrashHandler()
	return &c
}


//每次分发任务给worker后启用另一个线程检查任务是否超时
func (c *Coordinator) CheckTimeout(jobId int){
	time.Sleep(time.Second * time.Duration(10))
	mu.Lock()
	if c.CoordinatorCondition == AllDone {
		mu.Unlock()
		return
	}
	defer mu.Unlock()
	ok, jobInfo := c.jobMetaHolder.getJobMetaInfo(jobId)
	if !ok {  //该任务必须存在 且 处于等待开始 状态
		return
	}
	if jobInfo.condition == JobWorking{
		switch jobInfo.JobPtr.JobType {
		case MapJob:
			c.JobChannelMap <- jobInfo.JobPtr
			jobInfo.condition = JobWaiting
		case ReduceJob:
			c.JobChannelReduce <- jobInfo.JobPtr
			jobInfo.condition = JobWaiting

		}
	}
	
}
//响应worker的申请任务请求，负责分发任务，其实不需要使用exampleargs
//即worker提出请求，不需要参数，coordinator返回任务
func (c *Coordinator) DistributeJob(args *ExampleArgs, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	//fmt.Println("coordinator get a request from worker :")
	if c.CoordinatorCondition == MapPhase {    //map阶段
		if len(c.JobChannelMap) > 0 {
			*reply = *<-c.JobChannelMap
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {  //检查该任务是否能被分发
				//fmt.Printf("[duplicated job id]job %d is running\n", reply.JobId)
			}
			go c.CheckTimeout(reply.JobId)
		} else {
			reply.JobType = WaittingJob   //当前没有空闲的map任务了
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()   //进入reduce阶段
			}
			return nil
		}
	} else if c.CoordinatorCondition == ReducePhase {
		if len(c.JobChannelReduce) > 0 {
			*reply = *<-c.JobChannelReduce
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				//fmt.Printf("job %d is running\n", reply.JobId)
			}
			go c.CheckTimeout(reply.JobId)
		} else {
			reply.JobType = WaittingJob
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()
			}
			return nil
		}
	} else {   //所有任务都已完成/执行
		reply.JobType = KillJob   
	}
	return nil

}

//响应worker的任务完成反馈
func (c *Coordinator) JobIsDone(args *Job, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.JobType {
	case MapJob:
		ok, meta := c.jobMetaHolder.getJobMetaInfo(args.JobId)
		//prevent a duplicated work which returned from another worker
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone    //标记为完成
			//fmt.Printf("Map task on %d complete\n", args.JobId)
		} else {
			//fmt.Println("[duplicated] job done", args.JobId)
		}
		break
	case ReduceJob:
		//fmt.Printf("Reduce task on %d complete\n", args.JobId)
		ok, meta := c.jobMetaHolder.getJobMetaInfo(args.JobId)
		//prevent a duplicated work which returned from another worker
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
		} else {
			//fmt.Println("[duplicated] job done", args.JobId)
		}
		break
	default:
		//panic("wrong job done")
	}
	return nil

}

//获取下一个job序号
func (c *Coordinator) generateJobId() int {
	res := c.uniqueJobId
	c.uniqueJobId++
	return res
}
//当map或reduce完成后，coordinator控制进入下一阶段
func (c *Coordinator) nextPhase() {
	if c.CoordinatorCondition == MapPhase {
		//close(c.JobChannelMap)
		c.makeReduceJobs()   //准备reduce任务列
		c.CoordinatorCondition = ReducePhase
	} else if c.CoordinatorCondition == ReducePhase {
		//close(c.JobChannelReduce)
		c.CoordinatorCondition = AllDone
	}
}

//before serve，主节点制作任务
func (c *Coordinator) makeMapJobs(files []string) {
	for _, v := range files {
		id := c.generateJobId()
		//fmt.Println("making map job :", id)
		job := Job{
			JobType:    MapJob,
			InputFile:  []string{v},
			JobId:      id,
			ReducerNum: c.ReducerNum,
		}
		jobMetaINfo := JobMetaInfo{   //等待状态，time暂时没写
			condition: JobWaiting,
			JobPtr:    &job,
		}
		c.jobMetaHolder.putJob(&jobMetaINfo)   //写入任务堆中
		//fmt.Println("making map job :", &job)
		c.JobChannelMap <- &job      //写入消息队列
	}
	//defer close(c.JobChannelMap)
	//fmt.Println("done making map jobs")
	c.jobMetaHolder.checkJobDone()
}

func (c *Coordinator) makeReduceJobs() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateJobId()
		//fmt.Println("making reduce job :", id)
		JobToDo := Job{
			JobType:   ReduceJob,
			JobId:     id,
			InputFile: TmpFileAssignHelper(i, "main/mr-tmp"),   //其实不是从main/mr-tmp目录下找的
		}
		jobMetaInfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &JobToDo,
		}
		c.jobMetaHolder.putJob(&jobMetaInfo)
		c.JobChannelReduce <- &JobToDo

	}
	//defer close(c.JobChannelReduce)
	//fmt.Println("done making reduce jobs")
	c.jobMetaHolder.checkJobDone()
}
//任务写进元数据管理
func (j *JobMetaHolder) putJob(JobInfo *JobMetaInfo) bool {
	jobId := JobInfo.JobPtr.JobId   
	meta, _ := j.MetaMap[jobId]
	if meta != nil {   //检查冲突
		//fmt.Println("meta contains job which id = ", jobId)
		return false
	} else {
		j.MetaMap[jobId] = JobInfo
	}
	return true
}
//统计所有任务的完成情况
func (j *JobMetaHolder) checkJobDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == MapJob {
			if v.condition == JobDone {
				mapDoneNum += 1
			} else {
				mapUndoneNum++
			}
		} else {
			if v.condition == JobDone {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	//fmt.Printf("%d/%d map jobs are done, %d/%d reduce job are done\n",
	//	mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	if (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0) {
		return true
	}

	return false
}

func (j *JobMetaHolder) getJobMetaInfo(jobId int) (bool, *JobMetaInfo) {
	res, ok := j.MetaMap[jobId]
	return ok, res
}
//元数据管理器
func (j *JobMetaHolder) fireTheJob(jobId int) bool {
	ok, jobInfo := j.getJobMetaInfo(jobId)
	if !ok || jobInfo.condition != JobWaiting {  //该任务必须存在 且 处于等待开始 状态
		return false
	}
	jobInfo.condition = JobWorking
	jobInfo.StartTime = time.Now()
	return true
}

//寻找指定目录下的临时文件
func TmpFileAssignHelper(whichReduce int, tmpFileDirectoryName string) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)   //查找当前路径下的所有文件
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(whichReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}