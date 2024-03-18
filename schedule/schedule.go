package schedule

import (
	"fmt"
	"interact/accesslist"
	"interact/core"
	interactState "interact/state"
	"interact/tracer"
	"interact/utils"
	"math"
	"sync"
	"time"

	ethState "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/panjf2000/ants/v2"
)

type ScheduleRes struct {
	Flag   uint // 1 for CC , 2 for DAG, 3 for MIS
	cost   uint64
	txs    []types.Transactions
	rwsets []accesslist.RWSetList
	groups [][]uint
}

type PipeLineExecutor struct {
	sch chan *ScheduleRes

	wg sync.WaitGroup // for go-routine
}

func NewPipeLineExecutor() *PipeLineExecutor {
	pe := &PipeLineExecutor{
		sch: make(chan *ScheduleRes, 1000),
	}

	// pe.wg.Add(1)
	// go pe.PipeLineExecLoop()
	return pe
}

func (pe *PipeLineExecutor) PipeLineSchedule(txs types.Transactions, predictRwSets []*accesslist.RWSet) error {
	var wg sync.WaitGroup
	resultCh := make(chan ScheduleRes, 3)
	wg.Add(3)

	// fmt.Println("CC")
	go CC(txs, predictRwSets, &wg, resultCh)
	// fmt.Println("DAG")
	go DAG(txs, predictRwSets, &wg, resultCh)
	// fmt.Println("MIS")
	go MIS(txs, predictRwSets, &wg, resultCh)

	wg.Wait()
	close(resultCh)

	// 获取调度方案
	fmt.Println("get result")
	var minCost uint64 = math.MaxUint64
	finalRes := new(ScheduleRes)
	for res := range resultCh {
		if res.cost < minCost {
			minCost = res.cost
			temp := res
			finalRes = &temp
		}
	}

	pe.sch <- finalRes

	return nil
}

// func (pe *PipeLineExecutor) PipeLineExecLoop() {
// 	defer pe.wg.Done()
// 	for {
// 		select{
// 		case sr := <-pe.sch

// 		}
// 	}
// }

// func (pe *PipeLineExecutor) PipeLineExec(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
// 	var wg sync.WaitGroup
// 	var r sync.Mutex

// 	for i := blockNum; i < blockNum+100; i++ {
// 		// 1. getTransacion (seq )
// 		txs, predictRWSet, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)
// 		// 2. schedule
// 		st := time.Now()
// 		sr := Schedule(txs, predictRWSet)
// 		scheduleTime := time.Since(st)
// 		pe.sch <- sr
// 		state, _ := utils.GetState(chainDB, sdbBackend, blockNum)

// 		r.Lock()
// 		defer r.Unlock()

// 		switch sr.Flag {
// 		case 1:
// 			prefetchTime, executeTime, mergeTime, _ := schedule.SCC(sr, header, fakeChainCtx, state)
// 			totalTime := int64(scheduleTime.Microseconds()) + prefetchTime + executeTime + mergeTime
// 			fmt.Println("prefetchTime:", prefetchTime, "executeTime:", executeTime, "mergeTime:", mergeTime, "totalTime:", totalTime)
// 		case 2:
// 			prefetchTime, executeTime, mergeTime, _ := schedule.SDAG(sr, txs, predictRWSet, header, fakeChainCtx, state)
// 			totalTime := int64(scheduleTime.Microseconds()) + prefetchTime + executeTime + mergeTime
// 			fmt.Println("prefetchTime:", prefetchTime, "executeTime:", executeTime, "mergeTime:", mergeTime, "totalTime:", totalTime)
// 		case 3:
// 			prefetchTime, executeTime, mergeTime, _ := schedule.SMIS(sr, txs, predictRWSet, header, fakeChainCtx, state)
// 			totalTime := int64(scheduleTime.Microseconds()) + prefetchTime + executeTime + mergeTime
// 			fmt.Println("prefetchTime:", prefetchTime, "executeTime:", executeTime, "mergeTime:", mergeTime, "totalTime:", totalTime)
// 		default:
// 			panic("flag error")
// 		}

// 		wg.Done()

// 	}
// 	return nil
// }

func Schedule(txs types.Transactions, predictRwSets []*accesslist.RWSet) *ScheduleRes {

	var wg sync.WaitGroup
	resultCh := make(chan ScheduleRes, 3)
	wg.Add(3)

	// fmt.Println("CC")
	go CC(txs, predictRwSets, &wg, resultCh)
	// fmt.Println("DAG")
	go DAG(txs, predictRwSets, &wg, resultCh)
	// fmt.Println("MIS")
	go MIS(txs, predictRwSets, &wg, resultCh)

	wg.Wait()
	close(resultCh)

	// 获取调度方案
	fmt.Println("get result")
	var minCost uint64 = math.MaxUint64
	finalRes := new(ScheduleRes)
	for res := range resultCh {
		if res.cost < minCost {
			minCost = res.cost
			temp := res
			finalRes = &temp
		}
	}

	return finalRes
}

// CC 连通分量调度估算
func CC(txs types.Transactions, predictRwSets []*accesslist.RWSet, wg *sync.WaitGroup, resultCh chan<- ScheduleRes) {
	defer wg.Done()
	// 无向图
	vertexGroup := utils.GenerateVertexGroups(txs, predictRwSets)
	// 分组
	txsGroup, RWSetsGroup := utils.GenerateCCGroups(vertexGroup, txs, predictRwSets)
	// 获取最大cost
	var maxCost uint64
	for i := 0; i < len(txsGroup); i++ {
		var temp uint64
		for j := 0; j < len(txsGroup[i]); j++ {
			temp = temp + txsGroup[i][j].Gas()
		}
		if temp > maxCost {
			maxCost = temp
		}
	}
	fmt.Println("cc maxCost:", maxCost)
	// 构造返回结构体
	Res := ScheduleRes{
		Flag:   1,
		cost:   maxCost,
		txs:    txsGroup,
		rwsets: RWSetsGroup,
		groups: nil,
	}
	// fmt.Println("Res:", Res)
	resultCh <- Res
}

// DAG 有向无环图调度估算
func DAG(txs types.Transactions, predictRwSets []*accesslist.RWSet, wg *sync.WaitGroup, resultCh chan<- ScheduleRes) {
	defer wg.Done()
	groups := utils.GenerateDegreeZeroGroups(txs, predictRwSets)
	// 获取最大cost
	var maxCost uint64
	maxCost = 0
	for i := 0; i < len(groups); i++ {
		temp := txs[groups[i][0]].Gas()
		for j := 1; j < len(groups[i]); j++ {
			if temp < txs[groups[i][j]].Gas() {
				temp = txs[groups[i][j]].Gas()
			}
		}
		maxCost += temp
	}
	fmt.Println("dag maxCost:", maxCost)
	// 构造返回结构体
	Res := ScheduleRes{
		Flag:   2,
		cost:   maxCost,
		txs:    nil,
		rwsets: nil,
		groups: groups,
	}
	resultCh <- Res
}

// MIS 最大独立集调度估算
func MIS(txs types.Transactions, predictRwSets []*accesslist.RWSet, wg *sync.WaitGroup, resultCh chan<- ScheduleRes) {
	defer wg.Done()
	// 分组
	groups := utils.GenerateMISGroups(txs, predictRwSets)
	// 获取最大cost
	var maxCost uint64
	maxCost = 0
	for i := 0; i < len(groups); i++ {
		temp := txs[groups[i][0]].Gas()
		for j := 1; j < len(groups[i]); j++ {
			if temp < txs[groups[i][j]].Gas() {
				temp = txs[groups[i][j]].Gas()
			}
		}
		maxCost += temp
	}
	fmt.Println("mis maxCost:", maxCost)
	// 构造返回结构体
	Res := ScheduleRes{
		Flag:   3,
		cost:   maxCost,
		txs:    nil,
		rwsets: nil,
		groups: groups,
	}
	resultCh <- Res
}

func SCC(sr *ScheduleRes, header *types.Header, fakeChainCtx core.ChainContext, state *ethState.StateDB) (int64, int64, int64, error) {
	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	// 预取
	prefectStart := time.Now()
	cacheStates := utils.GenerateCacheStates(state, sr.rwsets)
	prefectTime := time.Since(prefectStart)

	// 执行
	execStart := time.Now()
	tracer.ExecConflictedTxs(antsPool, sr.txs, cacheStates, header, fakeChainCtx, &antsWG)
	execTime := time.Since(execStart)

	// 合并
	mergeStart := time.Now()
	utils.MergeToState(cacheStates, state)
	mergeTime := time.Since(mergeStart)

	// 预取时间，执行时间，合并时间，总时间
	return int64(prefectTime.Microseconds()), int64(execTime.Microseconds()), int64(mergeTime.Microseconds()), nil
}

func SDAG(sr *ScheduleRes, txs types.Transactions, predictRwSets accesslist.RWSetList, header *types.Header, fakeChainCtx core.ChainContext, state *ethState.StateDB) (int64, int64, int64, error) {
	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	fullcache := interactState.NewFullCacheConcurrent()
	// here we don't pre warm the data
	fullcache.Prefetch(state, predictRwSets)
	PureExecutionCost := time.Duration(0)
	PurePrefetchCost := time.Duration(0) // without considering the very first fullcache prefetch
	PureMergeCost := time.Duration(0)
	for round := 0; round < len(sr.groups); round++ {
		// here we can add logic if len(groups[round]) if less than a threshold
		// fmt.Println("parallel exec and commit:", len(groups[round]))
		// Create groups to execute
		prefst := time.Now()
		txsToExec, cacheStates := utils.GenerateTxsAndCacheStatesWithAnts(antsPool, fullcache, sr.groups[round], txs, predictRwSets, &antsWG)
		PurePrefetchCost += time.Since(prefst)

		execst := time.Now()
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, cacheStates, header, fakeChainCtx, &antsWG)
		PureExecutionCost += time.Since(execst)
		mergest := time.Now()
		utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
		PureMergeCost += time.Since(mergest)
	}

	// 预取时间，执行时间，合并时间，总时间
	return int64(PurePrefetchCost.Microseconds()), int64(PureExecutionCost.Microseconds()), int64(PureMergeCost.Microseconds()), nil

}

func SMIS(sr *ScheduleRes, txs types.Transactions, predictRwSets accesslist.RWSetList, header *types.Header, fakeChainCtx core.ChainContext, state *ethState.StateDB) (int64, int64, int64, error) {
	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	fullcache := interactState.NewFullCacheConcurrent()
	// here we don't pre warm the data
	fullcache.Prefetch(state, predictRwSets)
	// st := time.Now()
	PureExecutionCost := time.Duration(0)
	PurePrefetchCost := time.Duration(0) // without considering the very first fullcache prefetch
	PureMergeCost := time.Duration(0)
	for round := 0; round < len(sr.groups); round++ {
		// here we can add logic if len(groups[round]) if less than a threshold

		// Create groups to execute
		// fmt.Println("parallel exec and commit:", len(groups[round]))
		prefst := time.Now()
		txsToExec, cacheStates := utils.GenerateTxsAndCacheStatesWithAnts(antsPool, fullcache, sr.groups[round], txs, predictRwSets, &antsWG)
		PurePrefetchCost += time.Since(prefst)

		execst := time.Now()
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, cacheStates, header, fakeChainCtx, &antsWG)
		PureExecutionCost += time.Since(execst)
		// fmt.Println("exec time:", time.Since(execst))

		mergest := time.Now()
		utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
		PureMergeCost += time.Since(mergest)
	}

	// 预取时间，执行时间，合并时间，总时间
	return int64(PurePrefetchCost.Microseconds()), int64(PureExecutionCost.Microseconds()), int64(PureMergeCost.Microseconds()), nil
}
