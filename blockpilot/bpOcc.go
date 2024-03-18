package blockpilot

import (
	"fmt"
	"interact/accesslist"
	"interact/core"
	interactState "interact/state"
	"interact/tracer"
	"interact/utils"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	ethState "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/golang-collections/collections/stack"
	"github.com/panjf2000/ants/v2"
)

var BPMUTEX sync.Mutex
var Table map[common.Hash]int
var txStack = stack.New()

func BlockPilot(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
	// 获取Tx和初始的stateDB
	txs, _, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)

	state, err := utils.GetState(chainDB, sdbBackend, blockNum-1)
	if err != nil {
		return err
	}

	// 将txs转化成堆栈
	for _, tx := range txs {
		txStack.Push(tx)
	}

	// 初始化一个全局表Table = map[key]version
	Table = make(map[common.Hash]int)

	// 准备线程池
	var wg sync.WaitGroup
	pool, err := ants.NewPool(6)
	if err != nil {
		fmt.Printf("Failed to create pool: %v\n", err)
		return err
	}

	// 循环执行交易
	for i := 0; i < txs.Len(); i++ {
		wg.Add(1)

		// 提交任务到线程池
		err := pool.Submit(func() {
			// tx <- popHead
			tx := txStack.Pop().(*types.Transaction)
			// snapshot
			version := state.Snapshot()
			cacheState := state.Copy()

			// rs,ws <- Execute
			rws, err := ExecuteTx(tx, cacheState, header, fakeChainCtx)
			if err != nil {
				panic(err)
			}

			// DetectConflict
			DetectConflict(tx, rws, version)

			wg.Done()
		})

		if err != nil {
			fmt.Printf("Failed to submit task: %v\n", err)
			break
		}
	}

	return nil
}

func ExecuteTx(tx *types.Transaction, sdb *ethState.StateDB, header *types.Header, ctx core.ChainContext) (*accesslist.RWSet, error) {
	fulldb := interactState.NewStateWithRwSets(sdb)
	rws, err := tracer.ExecToGenerateRWSet(fulldb, tx, header, ctx)
	if err != nil {
		return nil, err
	}
	return rws, nil
}

func DetectConflict(tx *types.Transaction, rws *accesslist.RWSet, snapshotVersion int) bool {
	// 检查键是否存在
	for _, sValue := range rws.ReadSet {
		BPMUTEX.Lock()
		for key, _ := range sValue {
			if version, ok := Table[key]; ok {
				// 若key存在，则比较version
				if version > snapshotVersion {
					txStack.Push(tx)
					return false
				}
			} else {
				Table[key] = snapshotVersion
			}
		}
	}

	return true
}
