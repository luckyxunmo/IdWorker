package main

import (
	"fmt"
	"time"
)

// Snowflake算法
var(
	// 起始时间，单位是ms
	epoch int64 = 1563278591000

	//机器ID 所占的位数
	workerIdBits uint = 5

	//数据标识ID所占的位数
	dataCenterIdBits uint = 5

	//支持的最大机器ID，31
	maxWorkerID int64 = ^(-1 << workerIdBits)

	//支持的最大数据标识ID，31
	maxDataCenterId int64 = ^(-1  << dataCenterIdBits)

	// 毫秒系列在 id中所占的位数
	sequenceBits uint = 12

	//机器ID 左移的位数
	workerIdShift = sequenceBits

	// 数据标识ID 左移的位数
	dataCenterIdShift = sequenceBits + workerIdBits

	//时间戳左移
	timestampShift = sequenceBits + workerIdBits + dataCenterIdBits

	//生成系列的掩码，这里是4095 (0b111111111111=0xfff=4095)
	sequenceMask int64 = ^(-1 << sequenceBits)
)

type IdWorker struct {
	DataCenterID int64
	WorkerId int64
	Sequence int64
	LastTimestamp int64
	ids   chan int64
	stop chan struct{}
}

func NewIdWorker(dataCenterId,workerId int64) (*IdWorker,error)  {
    if dataCenterId > maxDataCenterId || dataCenterId < 0{
    	err := fmt.Errorf("dataCenterId can't be greater than %d or less than 0",maxDataCenterId)
    	return nil,err
	}
	if workerId > maxWorkerID || workerId < 0{
		err := fmt.Errorf("workerId can't be greater than %d or less than 0",maxWorkerID)
		return nil,err
	}
	return &IdWorker{
		DataCenterID:dataCenterId,
		WorkerId:workerId,
		ids : make(chan int64,5000),
	},nil

}
func (iw *IdWorker)Start()  {
  go iw.GenerateId()
}
func (iw *IdWorker)Close()   {
    select{
    case <- iw.stop:
    	return
	}
	close(iw.stop)
}

func (iw *IdWorker) NextMillis(lastTimestamp int64) int64  {
	ts := timeGen()
	return ts
}

func timeGen() int64  {
	return time.Now().UnixNano()/ 1e6
}

func (iw *IdWorker) NextId() int64  {
	timestamp := timeGen()
	if timestamp < iw.LastTimestamp{
		fmt.Errorf("clock is back")
		return 0
	}
	// 在同一时间生成，则进行毫秒内序列
	if timestamp == iw.LastTimestamp{
		iw.Sequence = (iw.Sequence +1) & sequenceMask
		if iw.Sequence == 0{
			timestamp = iw.NextMillis(iw.LastTimestamp)
		}
	}else{
		iw.Sequence = 0
	}

	iw.LastTimestamp = timestamp

	return ((timestamp-epoch)<< timestampShift)|
		   (iw.DataCenterID << dataCenterIdShift) |
		  (iw.WorkerId << workerIdShift) |
		  iw.Sequence


}

func (iw *IdWorker) GetId() int64  {
	return  <- iw.ids
}
func (iw *IdWorker)GenerateId()  {
	for{
		select{
         case <- iw.stop:
         	return
		default:
			id := iw.NextId()
			iw.ids <- id
		}

	}
}

func main() {
  test1()
  test2()
}

func test1()  {
	t := time.Now().UnixNano()
	w,_ := NewIdWorker(2,1)
	for i := 0; i < 5;i++{
		t := w.NextId()
		fmt.Printf("%d:%b\n",t,t)
	}

	fmt.Println( "test1:",time.Now().UnixNano() - t)
}

func test2()  {
	t := time.Now().UnixNano()

	w,_ := NewIdWorker(1,1)
	for i := 0; i < 5;i++{
		t := w.NextId()
		fmt.Printf("%d:%b\n",t,t)
	}

	fmt.Println( "tes2:",time.Now().UnixNano() - t)
}


func test3()  {
	t := time.Now().UnixNano()
	w,_ := NewIdWorker(1,1)
	w.Start()
	for i := 0; i < 5;i++{
		w.GetId()

	}
	fmt.Println( "test2:",time.Now().UnixNano() - t)
}
