package testfunc

import (
	"fmt"
	"interact/accesslist"
	"interact/core"
	interactState "interact/state"
	"interact/tracer"
	"interact/utils"
	"sync"
	"time"

	ethState "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/panjf2000/ants/v2"
)

// 串行
func SerialTest(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) (int64, int, error) {
	fmt.Println("Serial Execution")

	txs, _, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)

	// get statedb from block[startNum-1].Root
	state, err := utils.GetState(chainDB, sdbBackend, blockNum-1)
	if err != nil {
		return 0, 0, err
	}
	state.StartPrefetcher("miner")
	defer state.StopPrefetcher()

	// set the satrt time
	start := time.Now()
	// test the serial execution
	for i := 0; i < len(txs); i++ {
		tracer.ExecuteTxs(state, txs, header, fakeChainCtx)
	}
	// cal the execution time
	elapsed := time.Since(start)
	fmt.Println("Serial Execution Time:", elapsed)

	return int64(elapsed.Microseconds()), len(txs), nil
}

// PrefetchTest1 串行预取合并（全冷）
func PrefetchTest1(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) (int64, int64, error) {
	fmt.Println("Serial Prefetch Test totally cold")

	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)

	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	// get statedb from block[startNum-1].Root
	state, err := utils.GetState(chainDB, sdbBackend, blockNum-1)
	if err != nil {
		return 0, 0, err
	}

	txGroupsList, RWSetGroupsList := utils.GenerateTxAndRWSetGroups(txs, predictRwSets)

	// 全冷预取
	prefectStart := time.Now()
	cacheStates := utils.GenerateCacheStates(state, RWSetGroupsList)
	prefectTime := time.Since(prefectStart)
	// fmt.Println("Prefetch Time:", prefectTime)

	tracer.ExecConflictedTxs(antsPool, txGroupsList, cacheStates, header, fakeChainCtx, &antsWG)

	mergeStart := time.Now()
	utils.MergeToState(cacheStates, state)
	mergeTime := time.Since(mergeStart)

	return int64(prefectTime.Microseconds()), int64(mergeTime.Microseconds()), nil
}

// PrefetchTest2 串行预取合并（全热）
func PrefetchTest2(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) (int64, int64, error) {
	fmt.Println("Serial Prefetch Test totally warm")

	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)

	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	// get statedb from block[startNum-1].Root
	state, err := utils.GetState(chainDB, sdbBackend, blockNum-1)
	if err != nil {
		return 0, 0, err
	}

	txGroupsList, RWSetGroupsList := utils.GenerateTxAndRWSetGroups(txs, predictRwSets)

	utils.GenerateCacheStates(state, RWSetGroupsList)
	// 全热预取
	prefectStart := time.Now()
	cacheStates := utils.GenerateCacheStates(state, RWSetGroupsList)
	prefectTime := time.Since(prefectStart)
	// fmt.Println("Prefetch Time:", prefectTime)

	tracer.ExecConflictedTxs(antsPool, txGroupsList, cacheStates, header, fakeChainCtx, &antsWG)

	mergeStart := time.Now()
	utils.MergeToState(cacheStates, state)
	mergeTime := time.Since(mergeStart)

	return int64(prefectTime.Microseconds()), int64(mergeTime.Microseconds()), nil
}

// PrefetchTest3 串行预取合并（预热128区块）
func PrefetchTest3(state *ethState.StateDB, chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) (int64, int64, error) {
	fmt.Println("Serial Prefetch Test with 128 blocks warm")

	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)

	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	txGroupsList, RWSetGroupsList := utils.GenerateTxAndRWSetGroups(txs, predictRwSets)

	// 部分预热预取
	prefectStart := time.Now()
	cacheStates := utils.GenerateCacheStates(state, RWSetGroupsList)
	prefectTime := time.Since(prefectStart)
	// fmt.Println("Prefetch Time:", prefectTime)

	tracer.ExecConflictedTxs(antsPool, txGroupsList, cacheStates, header, fakeChainCtx, &antsWG)

	mergeStart := time.Now()
	utils.MergeToState(cacheStates, state)
	mergeTime := time.Since(mergeStart)

	return int64(prefectTime.Microseconds()), int64(mergeTime.Microseconds()), nil
}

// PrefetchTest4 并行预取合并
func PrefetchTest4(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) (int64, int64, error) {
	fmt.Println("Parallel Prefetch Test")

	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)

	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	// get statedb from block[startNum-1].Root
	state, err := utils.GetState(chainDB, sdbBackend, blockNum-1)
	if err != nil {
		return 0, 0, err
	}

	txGroupsList, RWSetGroupsList := utils.GenerateTxAndRWSetGroups(txs, predictRwSets)
	fullcache := interactState.NewFullCacheConcurrent()
	// here we don't pre warm the data
	fullcache.Prefetch(state, predictRwSets)

	// 部分预热预取
	prefectStart := time.Now()
	cacheStates := utils.GenerateCacheStatesConcurrent(antsPool, fullcache, RWSetGroupsList, &antsWG)
	prefectTime := time.Since(prefectStart)
	// fmt.Println("Prefetch Time:", prefectTime)

	tracer.ExecConflictedTxs(antsPool, txGroupsList, cacheStates, header, fakeChainCtx, &antsWG)

	mergeStart := time.Now()
	utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
	mergeTime := time.Since(mergeStart)

	return int64(prefectTime.Microseconds()), int64(mergeTime.Microseconds()), nil
}

// 连通分量（预热128block）
func CCTest1(txs types.Transactions, predictRWSet accesslist.RWSetList, header *types.Header, fakeChainCtx core.ChainContext, state *ethState.StateDB) (int64, int64, int64, int64, int64, int64, int64, error) {

	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	// 建图分组
	graphStart := time.Now()
	graph := utils.GenerateVertexGroups(txs, predictRWSet)
	graphTime := time.Since(graphStart)

	groupstart := time.Now()
	txGroup, RwSetGroup := utils.GenerateCCGroups(graph, txs, predictRWSet)
	// txGroup, RwSetGroup := utils.GenerateTxAndRWSetGroups(txs, predictRWSet)
	groupTime := time.Since(groupstart)
	createGraphTime := time.Since(graphStart)

	// 预取
	prefectStart := time.Now()
	cacheStates := utils.GenerateCacheStates(state, RwSetGroup)
	prefectTime := time.Since(prefectStart)

	// 执行
	execStart := time.Now()
	tracer.ExecConflictedTxs(antsPool, txGroup, cacheStates, header, fakeChainCtx, &antsWG)
	execTime := time.Since(execStart)

	// 合并
	mergeStart := time.Now()
	utils.MergeToState(cacheStates, state)
	mergeTime := time.Since(mergeStart)

	// 总时间
	timeSum := time.Since(graphStart)

	// 返回建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	return int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(prefectTime.Microseconds()), int64(execTime.Microseconds()), int64(mergeTime.Microseconds()), int64(timeSum.Microseconds()), nil
}

// 连通分量（并发预取合并）
func CCTest2(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) (int, int64, int64, int64, int64, int64, int64, int64, error) {
	fmt.Println("ConnectedComponent Execution")

	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)

	// 准备线程池
	var antsWG sync.WaitGroup
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()

	// 建图
	// 建图分组
	graphStart := time.Now()
	graph := utils.GenerateVertexGroups(txs, predictRwSets)
	graphTime := time.Since(graphStart)

	groupstart := time.Now()
	txGroup, RwSetGroup := utils.GenerateCCGroups(graph, txs, predictRwSets)
	// txGroup, RwSetGroup := utils.GenerateTxAndRWSetGroups(txs, predictRWSet)
	groupTime := time.Since(groupstart)
	createGraphTime := time.Since(graphStart)

	// 准备状态数据库
	state, err := utils.GetState(chainDB, sdbBackend, blockNum-1)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, err
	}
	fullcache := interactState.NewFullCacheConcurrent()
	// here we don't pre warm the data
	fullcache.Prefetch(state, predictRwSets)

	// 并发预取
	prefectStart := time.Now()
	cacheStates := utils.GenerateCacheStatesConcurrent(antsPool, fullcache, RwSetGroup, &antsWG)
	prefectTime := time.Since(prefectStart)

	// 并发执行
	execStart := time.Now()
	tracer.ExecConflictedTxs(antsPool, txGroup, cacheStates, header, fakeChainCtx, &antsWG)
	execTime := time.Since(execStart)

	// 并发合并
	mergeStart := time.Now()
	utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
	mergeTime := time.Since(mergeStart)

	// 总时间
	timeSum := time.Since(graphStart)

	// 返回建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	return len(txs), int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(prefectTime.Microseconds()), int64(execTime.Microseconds()), int64(mergeTime.Microseconds()), int64(timeSum.Microseconds()), nil
}

// DAG （并发预取合并）
func DAGTest(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) (int, int64, int64, int64, int64, int64, int64, int64, error) {
	fmt.Println("DegreeZero Solution Concurrent Fullstate")

	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)

	state, err := utils.GetState(chainDB, sdbBackend, blockNum-1)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, err
	}

	// 准备线程池
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()
	var antsWG sync.WaitGroup

	// groups := utils.GenerateDegreeZeroGroups(txs, predictRwSets)
	// 建图
	graphStart := time.Now()
	graph := utils.GenerateDiGraph(txs, predictRwSets)
	graphTime := time.Since(graphStart)

	// 分组
	groupstart := time.Now()
	groups := graph.GetDegreeZero()
	// txGroup, RwSetGroup := utils.GenerateTxAndRWSetGroups(txs, predictRWSet)
	groupTime := time.Since(groupstart)
	createGraphTime := time.Since(graphStart)

	fullcache := interactState.NewFullCacheConcurrent()
	// here we don't pre warm the data
	fullcache.Prefetch(state, predictRwSets)
	// st := time.Now()
	PureExecutionCost := time.Duration(0)
	PurePrefetchCost := time.Duration(0) // without considering the very first fullcache prefetch
	PureMergeCost := time.Duration(0)
	for round := 0; round < len(groups); round++ {
		// here we can add logic if len(groups[round]) if less than a threshold
		// fmt.Println("parallel exec and commit:", len(groups[round]))
		// Create groups to execute
		prefst := time.Now()
		txsToExec, cacheStates := utils.GenerateTxsAndCacheStatesWithAnts(antsPool, fullcache, groups[round], txs, predictRwSets, &antsWG)
		PurePrefetchCost += time.Since(prefst)

		execst := time.Now()
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, cacheStates, header, fakeChainCtx, &antsWG)
		PureExecutionCost += time.Since(execst)
		mergest := time.Now()
		utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
		PureMergeCost += time.Since(mergest)
	}

	// execTime := time.Since(st)
	// 总时间
	timeSum := time.Since(graphStart)

	// 返回建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，多轮时间，总时间
	return len(txs), int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(PurePrefetchCost.Microseconds()), int64(PureExecutionCost.Microseconds()), int64(PureMergeCost.Microseconds()), int64(timeSum.Microseconds()), nil
}

// MIS (并发预取合并)
func MISTest(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) (int, int64, int64, int64, int64, int64, int64, int64, error) {
	fmt.Println("MIS Solution Concurrent CacheState")

	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)

	state, err := utils.GetState(chainDB, sdbBackend, blockNum-1)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, 0, 0, err
	}

	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()
	var antsWG sync.WaitGroup

	// st := time.Now()
	// groups := utils.GenerateMISGroups(txs, predictRwSets)
	// fmt.Println("Generate TxGroups:", time.Since(st))
	// 建图
	graphStart := time.Now()
	graph := utils.GenerateUndiGraph(txs, predictRwSets)
	graphTime := time.Since(graphStart)
	fmt.Println("graphtime:", graphTime)

	// 分组
	groupstart := time.Now()
	groups := utils.SolveMISInTurn(graph)
	// txGroup, RwSetGroup := utils.GenerateTxAndRWSetGroups(txs, predictRWSet)
	groupTime := time.Since(groupstart)
	createGraphTime := time.Since(graphStart)
	fmt.Println("grouptime:", groupTime)

	fullcache := interactState.NewFullCacheConcurrent()
	// here we don't pre warm the data
	fullcache.Prefetch(state, predictRwSets)
	// st := time.Now()
	PureExecutionCost := time.Duration(0)
	PurePrefetchCost := time.Duration(0) // without considering the very first fullcache prefetch
	PureMergeCost := time.Duration(0)
	for round := 0; round < len(groups); round++ {
		// here we can add logic if len(groups[round]) if less than a threshold

		// Create groups to execute
		// fmt.Println("parallel exec and commit:", len(groups[round]))
		prefst := time.Now()
		txsToExec, cacheStates := utils.GenerateTxsAndCacheStatesWithAnts(antsPool, fullcache, groups[round], txs, predictRwSets, &antsWG)
		PurePrefetchCost += time.Since(prefst)

		execst := time.Now()
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, cacheStates, header, fakeChainCtx, &antsWG)
		PureExecutionCost += time.Since(execst)
		// fmt.Println("exec time:", time.Since(execst))

		mergest := time.Now()
		utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
		PureMergeCost += time.Since(mergest)
	}

	// execTime := time.Since(st)
	// 总时间
	timeSum := time.Since(graphStart)

	// 返回建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，多轮时间，总时间
	return len(txs), int64(graphTime.Microseconds()), int64(groupTime.Microseconds()), int64(createGraphTime.Microseconds()), int64(PurePrefetchCost.Microseconds()), int64(PureExecutionCost.Microseconds()), int64(PureMergeCost.Microseconds()), int64(timeSum.Microseconds()), nil
}

// Aria then CC (并发预取合并)
func ATCC(chainDB ethdb.Database, sdbBackend ethState.Database, height uint64) (int, int, int, int64, int64, int64, error) {
	// 准备线程池
	fmt.Println("Aria Method Then Connected Components With One Block")
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()
	var antsWG sync.WaitGroup

	// execution environment
	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, height)
	trueRWlists, _ := TrueRWSets(txs, chainDB, sdbBackend, height)
	state, err := utils.GetState(chainDB, sdbBackend, height-1)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	fullcache := interactState.NewFullCacheConcurrent()
	fullcache.Prefetch(state, trueRWlists)
	fullcache.Prefetch(state, predictRwSets)

	// first we use Aria method to commit txs and get rw sets
	ar := time.Now()
	PrefetchRwSetList := make([]accesslist.RWSetList, len(txs))
	for i := range txs {
		PrefetchRwSetList[i] = accesslist.RWSetList{predictRwSets[i], trueRWlists[i]}
	}
	restTx, restPredictRwSets := utils.AriaOneRound(antsPool, txs, header, fakeChainCtx, fullcache, PrefetchRwSetList, &antsWG)
	artime := time.Since(ar)

	// then we use connected components method to commit the rest txs
	// fmt.Println("Start Connected Components Execution")
	st := time.Now()
	txGroup, RwSetGroup := utils.GenerateTxAndRWSetGroups(restTx, restPredictRwSets)
	cacheStates := utils.GenerateCacheStatesConcurrent(antsPool, fullcache, RwSetGroup, &antsWG)
	tracer.ExecConflictedTxs(antsPool, txGroup, cacheStates, header, fakeChainCtx, &antsWG)
	utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
	cctime := time.Since(st)
	sumtime := time.Since(ar)

	// tx数，Aria提交的交易，第一轮后剩余交易，执行总时间
	return len(txs), len(txs) - restTx.Len(), restTx.Len(), int64(artime.Microseconds()), int64(cctime.Microseconds()), int64(sumtime.Microseconds()), nil
}

func ATDAG(chainDB ethdb.Database, sdbBackend ethState.Database, height uint64) (int, int, int, int64, int64, int64, error) {
	// 准备线程池
	fmt.Println("Aria Method Then DAG With One Block")
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()
	var antsWG sync.WaitGroup

	// execution environment
	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, height)
	trueRWlists, _ := TrueRWSets(txs, chainDB, sdbBackend, height)
	state, err := utils.GetState(chainDB, sdbBackend, height-1)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	fullcache := interactState.NewFullCacheConcurrent()
	fullcache.Prefetch(state, trueRWlists)
	fullcache.Prefetch(state, predictRwSets)

	// first we use Aria method to commit txs and get rw sets
	ar := time.Now()
	PrefetchRwSetList := make([]accesslist.RWSetList, len(txs))
	for i := range txs {
		PrefetchRwSetList[i] = accesslist.RWSetList{predictRwSets[i], trueRWlists[i]}
	}
	restTx, restPredictRwSets := utils.AriaOneRound(antsPool, txs, header, fakeChainCtx, fullcache, PrefetchRwSetList, &antsWG)
	artime := time.Since(ar)

	st := time.Now()
	groups := utils.GenerateDegreeZeroGroups(restTx, restPredictRwSets)
	for round := 0; round < len(groups); round++ {
		txsToExec, cacheStates := utils.GenerateTxsAndCacheStatesWithAnts(antsPool, fullcache, groups[round], restTx, restPredictRwSets, &antsWG)
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, cacheStates, header, fakeChainCtx, &antsWG)
		utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
	}
	dagtime := time.Since(st)
	sumtime := time.Since(ar)
	return len(txs), len(txs) - restTx.Len(), restTx.Len(), int64(artime.Microseconds()), int64(dagtime.Microseconds()), int64(sumtime.Microseconds()), nil
}

func ATMIS(chainDB ethdb.Database, sdbBackend ethState.Database, height uint64) (int, int, int, int64, int64, int64, error) {
	// 准备线程池
	fmt.Println("Aria Method Then MIS With One Block")
	antsPool, _ := ants.NewPool(16, ants.WithPreAlloc(true))
	defer antsPool.Release()
	var antsWG sync.WaitGroup

	// execution environment
	txs, predictRwSets, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, height)
	trueRWlists, _ := TrueRWSets(txs, chainDB, sdbBackend, height)
	state, err := utils.GetState(chainDB, sdbBackend, height-1)
	if err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	fullcache := interactState.NewFullCacheConcurrent()
	fullcache.Prefetch(state, trueRWlists)
	fullcache.Prefetch(state, predictRwSets)

	// first we use Aria method to commit txs and get rw sets
	ar := time.Now()
	PrefetchRwSetList := make([]accesslist.RWSetList, len(txs))
	for i := range txs {
		PrefetchRwSetList[i] = accesslist.RWSetList{predictRwSets[i], trueRWlists[i]}
	}
	restTx, restPredictRwSets := utils.AriaOneRound(antsPool, txs, header, fakeChainCtx, fullcache, PrefetchRwSetList, &antsWG)
	artime := time.Since(ar)

	st := time.Now()
	groups := utils.GenerateMISGroups(restTx, restPredictRwSets)
	for round := 0; round < len(groups); round++ {
		txsToExec, cacheStates := utils.GenerateTxsAndCacheStatesWithAnts(antsPool, fullcache, groups[round], restTx, restPredictRwSets, &antsWG)
		tracer.ExecConflictFreeTxs(antsPool, txsToExec, cacheStates, header, fakeChainCtx, &antsWG)
		utils.MergeToCacheStateConcurrent(antsPool, cacheStates, fullcache, &antsWG)
	}
	mistime := time.Since(st)
	sumtime := time.Since(ar)
	return len(txs), len(txs) - restTx.Len(), restTx.Len(), int64(artime.Microseconds()), int64(mistime.Microseconds()), int64(sumtime.Microseconds()), nil
}
