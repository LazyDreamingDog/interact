package main

import (
	"encoding/csv"
	"fmt"
	"interact/schedule"
	"interact/utils"
	testfunc "interact/utils/testFunc"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	ethState "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/panjf2000/ants/v2"
)

func CC1(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
	fmt.Println("ConnectedComponent Execution")

	// 准备状态数据库（预热128block）只预热一次
	state := testfunc.WarmBlocks(chainDB, sdbBackend, blockNum)

	cc1file, err := os.Create(("cc1.csv"))
	if err != nil {
		panic(err)
	}
	defer cc1file.Close()
	cc1Writer := csv.NewWriter(cc1file)
	defer cc1Writer.Flush()
	// 建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	err = cc1Writer.Write([]string{"BlockNum", "TxNum", "graph", "group", "graph+group", "prefetch", "execute", "merge", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 100; i++ {
		txs, predictRWSet, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)
		blockNum := blockNum + 27 + uint64(i)
		fmt.Println("blockNum:", blockNum)
		// testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		graphTime, groupTime, graphGroupTime, prefectTime, executeTime, mergeTime, totalTime, _ := testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		err = cc1Writer.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(len(txs)), fmt.Sprint(graphTime), fmt.Sprint(groupTime), fmt.Sprint(graphGroupTime), fmt.Sprint(prefectTime), fmt.Sprint(executeTime), fmt.Sprint(mergeTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func CC2(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
	cc2file, err := os.Create(("cc2.csv"))
	if err != nil {
		panic(err)
	}
	defer cc2file.Close()
	cc2Writer := csv.NewWriter(cc2file)
	defer cc2Writer.Flush()
	// 建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	err = cc2Writer.Write([]string{"BlockNum", "TxNum", "graph", "group", "graph+group", "prefetch", "execute", "merge", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 100; i++ {
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		// testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		txsNum, graphTime, groupTime, graphGroupTime, prefectTime, executeTime, mergeTime, totalTime, _ := testfunc.CCTest2(chainDB, sdbBackend, blockNum)
		err = cc2Writer.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(txsNum), fmt.Sprint(graphTime), fmt.Sprint(groupTime), fmt.Sprint(graphGroupTime), fmt.Sprint(prefectTime), fmt.Sprint(executeTime), fmt.Sprint(mergeTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func DAG(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
	dagfile, err := os.Create(("dag.csv"))
	if err != nil {
		panic(err)
	}
	defer dagfile.Close()
	dagWriter := csv.NewWriter(dagfile)
	defer dagWriter.Flush()
	// 建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	err = dagWriter.Write([]string{"BlockNum", "TxNum", "graph", "group", "graph+group", "prefetch", "execute", "merge", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 100; i++ {
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		// testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		txsNum, graphTime, groupTime, graphGroupTime, prefectTime, executeTime, mergeTime, totalTime, _ := testfunc.DAGTest(chainDB, sdbBackend, blockNum)
		err = dagWriter.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(txsNum), fmt.Sprint(graphTime), fmt.Sprint(groupTime), fmt.Sprint(graphGroupTime), fmt.Sprint(prefectTime), fmt.Sprint(executeTime), fmt.Sprint(mergeTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func MIS(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
	misfile, err := os.Create(("mis.csv"))
	if err != nil {
		panic(err)
	}
	defer misfile.Close()
	misWriter := csv.NewWriter(misfile)
	defer misWriter.Flush()
	// 建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	err = misWriter.Write([]string{"BlockNum", "TxNum", "graph", "group", "graph+group", "prefetch", "execute", "merge", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 100; i++ {
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		// testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		txsNum, graphTime, groupTime, graphGroupTime, prefectTime, executeTime, mergeTime, totalTime, _ := testfunc.MISTest(chainDB, sdbBackend, blockNum)
		err = misWriter.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(txsNum), fmt.Sprint(graphTime), fmt.Sprint(groupTime), fmt.Sprint(graphGroupTime), fmt.Sprint(prefectTime), fmt.Sprint(executeTime), fmt.Sprint(mergeTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func PrefetchAndMergeTest(startBlockNum uint64, chainDB ethdb.Database, sdbBackend ethState.Database) {
	// 测试预取
	// 1 全冷
	pm1file, err := os.Create(("pm1.csv"))
	if err != nil {
		panic(err)
	}
	defer pm1file.Close()
	pm1Writer := csv.NewWriter(pm1file)
	defer pm1Writer.Flush()
	err = pm1Writer.Write([]string{"BlockNum", "prefetch", "merge"})
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		blockNum := startBlockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		prefectTime, mergeTime, _ := testfunc.PrefetchTest1(chainDB, sdbBackend, blockNum)
		err = pm1Writer.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(prefectTime), fmt.Sprint(mergeTime)})
		if err != nil {
			panic(err)
		}
	}

	// // 2 全热
	pm2file, err := os.Create(("pm2.csv"))
	if err != nil {
		panic(err)
	}
	defer pm2file.Close()
	pm2Writer := csv.NewWriter(pm2file)
	defer pm2Writer.Flush()
	err = pm2Writer.Write([]string{"BlockNum", "prefetch", "merge"})
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		blockNum := startBlockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		prefectTime, mergeTime, _ := testfunc.PrefetchTest2(chainDB, sdbBackend, blockNum)
		err = pm2Writer.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(prefectTime), fmt.Sprint(mergeTime)})
		if err != nil {
			panic(err)
		}
	}

	// 3 预热128
	pm3file, err := os.Create(("pm3.csv"))
	if err != nil {
		panic(err)
	}
	defer pm3file.Close()
	// 预热state
	// 准备状态数据库（预热128block）只预热一次
	state := testfunc.WarmBlocks(chainDB, sdbBackend, startBlockNum)
	pm3Writer := csv.NewWriter(pm3file)
	defer pm3Writer.Flush()
	err = pm3Writer.Write([]string{"BlockNum", "prefetch", "merge"})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 100; i++ {
		blockNum := startBlockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		prefectTime, mergeTime, _ := testfunc.PrefetchTest3(state, chainDB, sdbBackend, blockNum)
		err = pm3Writer.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(prefectTime), fmt.Sprint(mergeTime)})
		if err != nil {
			panic(err)
		}
	}

	// 4 并发
	pm4file, err := os.Create(("pm4.csv"))
	if err != nil {
		panic(err)
	}
	defer pm4file.Close()
	pm4Writer := csv.NewWriter(pm4file)
	defer pm4Writer.Flush()
	err = pm4Writer.Write([]string{"BlockNum", "prefetch", "merge"})
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		blockNum := startBlockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		prefectTime, mergeTime, _ := testfunc.PrefetchTest4(chainDB, sdbBackend, blockNum)
		err = pm4Writer.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(prefectTime), fmt.Sprint(mergeTime)})
		if err != nil {
			panic(err)
		}
	}
}

func Schedule(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) {
	// 准备状态数据库（预热128block）只预热一次
	// state := testfunc.WarmBlocks(chainDB, sdbBackend, blockNum)
	state, _ := utils.GetState(chainDB, sdbBackend, blockNum)
	for i := 0; i < 10; i++ {
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)

		txs, predictRWSet, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)
		fmt.Println("start schedule")
		st := time.Now()
		sr := schedule.Schedule(txs, predictRWSet)
		scheduleTime := time.Since(st)
		fmt.Println("flag:", sr.Flag)

		switch sr.Flag {
		case 1:
			prefetchTime, executeTime, mergeTime, _ := schedule.SCC(sr, header, fakeChainCtx, state)
			totalTime := int64(scheduleTime.Microseconds()) + prefetchTime + executeTime + mergeTime
			fmt.Println("prefetchTime:", prefetchTime, "executeTime:", executeTime, "mergeTime:", mergeTime, "totalTime:", totalTime)
		case 2:
			prefetchTime, executeTime, mergeTime, _ := schedule.SDAG(sr, txs, predictRWSet, header, fakeChainCtx, state)
			totalTime := int64(scheduleTime.Microseconds()) + prefetchTime + executeTime + mergeTime
			fmt.Println("prefetchTime:", prefetchTime, "executeTime:", executeTime, "mergeTime:", mergeTime, "totalTime:", totalTime)
		case 3:
			prefetchTime, executeTime, mergeTime, _ := schedule.SMIS(sr, txs, predictRWSet, header, fakeChainCtx, state)
			totalTime := int64(scheduleTime.Microseconds()) + prefetchTime + executeTime + mergeTime
			fmt.Println("prefetchTime:", prefetchTime, "executeTime:", executeTime, "mergeTime:", mergeTime, "totalTime:", totalTime)
		default:
			panic("flag error")
		}
	}

}

func pipelineSchedule(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
	var wg sync.WaitGroup
	var r sync.Mutex

	// 准备线程池
	pool, err := ants.NewPool(8)
	if err != nil {
		fmt.Printf("Failed to create pool: %v\n", err)
		return err
	}

	for i := blockNum; i < blockNum+100; i++ {
		wg.Add(1)

		err := pool.Submit(func() {
			// 1. getTransacion (seq )
			txs, predictRWSet, header, fakeChainCtx := utils.GetTxsPredictsAndHeadersForOneBlock(chainDB, sdbBackend, blockNum)
			// 2. schedule
			st := time.Now()
			sr := schedule.Schedule(txs, predictRWSet)
			scheduleTime := time.Since(st)

			state, _ := utils.GetState(chainDB, sdbBackend, blockNum)

			// exec with lock
			r.Lock()
			defer r.Unlock()

			switch sr.Flag {
			case 1:
				prefetchTime, executeTime, mergeTime, _ := schedule.SCC(sr, header, fakeChainCtx, state)
				totalTime := int64(scheduleTime.Microseconds()) + prefetchTime + executeTime + mergeTime
				fmt.Println("prefetchTime:", prefetchTime, "executeTime:", executeTime, "mergeTime:", mergeTime, "totalTime:", totalTime)
			case 2:
				prefetchTime, executeTime, mergeTime, _ := schedule.SDAG(sr, txs, predictRWSet, header, fakeChainCtx, state)
				totalTime := int64(scheduleTime.Microseconds()) + prefetchTime + executeTime + mergeTime
				fmt.Println("prefetchTime:", prefetchTime, "executeTime:", executeTime, "mergeTime:", mergeTime, "totalTime:", totalTime)
			case 3:
				prefetchTime, executeTime, mergeTime, _ := schedule.SMIS(sr, txs, predictRWSet, header, fakeChainCtx, state)
				totalTime := int64(scheduleTime.Microseconds()) + prefetchTime + executeTime + mergeTime
				fmt.Println("prefetchTime:", prefetchTime, "executeTime:", executeTime, "mergeTime:", mergeTime, "totalTime:", totalTime)
			default:
				panic("flag error")
			}

			wg.Done()
		})

		if err != nil {
			fmt.Printf("Failed to submit task: %v\n", err)
			break
		}
	}
	return nil
}

// Aria then cc
func ApexP1(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
	atccfile, err := os.Create(("atcc.csv"))
	if err != nil {
		panic(err)
	}
	defer atccfile.Close()
	atccWriter := csv.NewWriter(atccfile)
	defer atccWriter.Flush()
	// 建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	err = atccWriter.Write([]string{"BlockNum", "TxNum", "AriaTx", "RestTx", "AriaTime", "CCTime", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 100; i++ {
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		// testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		txsNum, ariaTxsNum, RestTxsNum, ariaTime, ccTime, totalTime, _ := testfunc.ATCC(chainDB, sdbBackend, blockNum)
		err = atccWriter.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(txsNum), fmt.Sprint(ariaTxsNum), fmt.Sprint(RestTxsNum), fmt.Sprint(ariaTime), fmt.Sprint(ccTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// Aria then dag
func ApexP2(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
	atdagfile, err := os.Create(("atdag.csv"))
	if err != nil {
		panic(err)
	}
	defer atdagfile.Close()
	atdagWriter := csv.NewWriter(atdagfile)
	defer atdagWriter.Flush()
	// 建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	err = atdagWriter.Write([]string{"BlockNum", "TxNum", "AriaTx", "RestTx", "AriaTime", "DAGTime", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 100; i++ {
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		// testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		txsNum, ariaTxsNum, RestTxsNum, ariaTime, dagTime, totalTime, _ := testfunc.ATDAG(chainDB, sdbBackend, blockNum)
		err = atdagWriter.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(txsNum), fmt.Sprint(ariaTxsNum), fmt.Sprint(RestTxsNum), fmt.Sprint(ariaTime), fmt.Sprint(dagTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// Ari then mis
func ApexP3(chainDB ethdb.Database, sdbBackend ethState.Database, blockNum uint64) error {
	atmisfile, err := os.Create(("atmis.csv"))
	if err != nil {
		panic(err)
	}
	defer atmisfile.Close()
	atmisWriter := csv.NewWriter(atmisfile)
	defer atmisWriter.Flush()
	// 建图时间，分组时间，建图分组总时间，预取时间，执行时间，合并时间，总时间
	err = atmisWriter.Write([]string{"BlockNum", "TxNum", "AriaTx", "RestTx", "AriaTime", "MISTime", "total"})
	if err != nil {
		panic(err)
	}

	fmt.Println("test start")
	for i := 0; i < 100; i++ {
		blockNum := blockNum + uint64(i)
		fmt.Println("blockNum:", blockNum)
		// testfunc.CCTest1(txs, predictRWSet, header, fakeChainCtx, state)
		txsNum, ariaTxsNum, RestTxsNum, ariaTime, misTime, totalTime, _ := testfunc.ATMIS(chainDB, sdbBackend, blockNum)
		err = atmisWriter.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(txsNum), fmt.Sprint(ariaTxsNum), fmt.Sprint(RestTxsNum), fmt.Sprint(ariaTime), fmt.Sprint(misTime), fmt.Sprint(totalTime)})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func main() {
	Node, chainDB, sdbBackend := utils.GetEthDatabaseAndStateDatabase()
	// Node, chainDB, _ := utils.GetEthDatabaseAndStateDatabase()
	defer Node.Close()
	head := rawdb.ReadHeadBlockHash(chainDB)
	num := *rawdb.ReadHeaderNumber(chainDB, head)
	fmt.Println("num:", num)
	// for i := num; ; i-- {
	// 	block, header := utils.GetBlockAndHeader(chainDB, i)
	// 	fmt.Println("block:", block.Number().Uint64(), "header:", header.Number.Uint64())
	// }
	var startBlockNum uint64 = num - 100
	// var startBlockNum uint64 = 14399431
	// var startBlockNum uint64 = 14399463
	// var startBlockNum uint64 = 14399488
	// var startBlockNum uint64 = 14399448
	// fmt.Println("startBlockNum:", startBlockNum)
	// serialtestfile, err := os.Create(("serial.csv"))
	// if err != nil {
	// 	panic(err)
	// }
	// defer serialtestfile.Close()
	// serialWriter := csv.NewWriter(serialtestfile)
	// defer serialWriter.Flush()
	// err = serialWriter.Write([]string{"BlockNum", "SerialTime", "TransactionNum"})
	// if err != nil {
	// 	panic(err)
	// }

	// // serial execution
	// for i := 0; i < 100; i++ {
	// 	blockNum := startBlockNum + uint64(i)
	// 	fmt.Println("blockNum:", blockNum)
	// 	serialTime, txsNum, _ := testfunc.SerialTest(chainDB, sdbBackend, blockNum)
	// 	err = serialWriter.Write([]string{fmt.Sprint(blockNum), fmt.Sprint(serialTime), fmt.Sprint(txsNum)})
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	// 预取合并测试
	// PrefetchAndMergeTest(startBlockNum, chainDB, sdbBackend)

	// cc测试
	// CC1(chainDB, sdbBackend, startBlockNum)

	// cc2测试
	// CC2(chainDB, sdbBackend, startBlockNum)

	// DAG 测试
	// DAG(chainDB, sdbBackend, startBlockNum)

	// MIS 测试
	// MIS(chainDB, sdbBackend, startBlockNum)

	// schedule
	Schedule(chainDB, sdbBackend, startBlockNum)
}
