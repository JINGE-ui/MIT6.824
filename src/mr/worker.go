package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "os"
import "encoding/json"
import "time"
import "strconv"
import "io/ioutil"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//为了reduce任务中的排序
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var workerId string = "0" 
	alive := true
	attempt := 0

	for alive {
		attempt++
		//fmt.Println(workerId, " >worker ask", attempt)

		job := RequireTask(workerId)
		//fmt.Println(workerId, "worker get job", job)
		switch job.JobType {
		case MapJob:
			{
				DoMap(mapf, job)
				//fmt.Println("do map", job.JobId)
				JobIsDone(workerId, job)
			}
		case ReduceJob:
			//if job.JobId >= 8 {
				DoReduce(reducef, job)
				//fmt.Println("do reduce", job.JobId)
				JobIsDone("kaka", job)
			//}

		case WaittingJob:
			//fmt.Println("get waiting")
			time.Sleep(time.Second)
		case KillJob:   //收到消息全部工作已完成
			{
				time.Sleep(time.Second)
				//fmt.Println("[Status] :", workerId, "terminated........")
				alive = false
			}
		}
		time.Sleep(time.Second)

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//完成任务后反馈给master
func JobIsDone(workerId string, job *Job) {
	args := job
	reply := &ExampleReply{}
	call("Coordinator.JobIsDone", &args, &reply)
}

//根据返回的Job作map任务，mapf使用的仍是wc.gp的函数
func DoMap(mapf func(string, string) []KeyValue, response *Job) {
	var intermediate []KeyValue
	filename := response.InputFile[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))

	//initialize and loop over []KeyValue
	rn := response.ReducerNum   //需要将结果分成nreduce份
	HashedKV := make([][]KeyValue, rn)   //创造切片，大小为nr，每个元素的类型是[]keyvalue

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}
	for i := 0; i < rn; i++ {   //写到当前目录下 mr-tmp-mapid-reduceid
		oname := "mr-tmp-" + strconv.Itoa(response.JobId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname+"-fail")
		enc := json.NewEncoder(ofile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		ofile.Close()
		os.Rename(oname+"-fail",oname)
	}

}

//reduce任务
func DoReduce(reducef func(string, []string) string, response *Job) {
	reduceFileNum := response.JobId
	intermediate := readFromLocalFile(response.InputFile)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	//tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), oname)
}

//reduce任务读取本地文件
func readFromLocalFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}

//向coordinator获取任务请求，调用call函数
func RequireTask(workerId string) *Job {
	args := ExampleArgs{}

	reply := Job{}

	call("Coordinator.DistributeJob", &args, &reply)
	//fmt.Println("get response", &reply)

	return &reply

}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	//见callExample的调用形式
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}
