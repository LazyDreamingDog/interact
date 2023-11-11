package main

import (
	"fmt"
	"interact/accesslist"
	conflictgraph "interact/conflictGraph"
	"interact/mis"
	"interact/tracer"

	"github.com/ethereum/go-ethereum/core/rawdb"
	statedb "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/triedb/pathdb"
)

func GetEthDatabaseAndStateDatabase() (*node.Node, ethdb.Database, statedb.Database) {
	nodeCfg := node.Config{DataDir: "/mnt/disk1/xsp/chaindata/execution/"}
	Node, err := node.New(&nodeCfg)
	if err != nil {
		panic(err)
	}
	ethCfg := ethconfig.Defaults
	chainDB, err := Node.OpenDatabaseWithFreezer("chaindata", ethCfg.DatabaseCache, ethCfg.DatabaseHandles, ethCfg.DatabaseFreezer, "eth/db/chaindata/", true)
	if err != nil {
		panic(err)
	}

	config := &trie.Config{Preimages: ethCfg.Preimages}
	config.PathDB = &pathdb.Config{
		StateHistory:   ethCfg.StateHistory,
		CleanCacheSize: 256 * 1024 * 1024,
		DirtyCacheSize: 256 * 1024 * 1024,
	}

	trieDB := trie.NewDatabase(chainDB, config)
	sdbBackend := statedb.NewDatabaseWithNodeDB(chainDB, trieDB)
	return Node, chainDB, sdbBackend
}

func PredictRWAL(tx *types.Transaction, chainDB ethdb.Database, sdbBackend statedb.Database, num uint64) *accesslist.RW_AccessLists {

	baseHeadHash := rawdb.ReadCanonicalHash(chainDB, num-1)
	baseHeader := rawdb.ReadHeader(chainDB, baseHeadHash, num-1)

	state, err := statedb.New(baseHeader.Root, sdbBackend, nil)
	if err != nil {
		panic(err)
	}

	headHash := rawdb.ReadCanonicalHash(chainDB, num)
	header := rawdb.ReadHeader(chainDB, headHash, num)
	list, _ := tracer.CreateRWAL(state, tx, header)
	// listJSON := list.ToJSON()
	// b := common.Hex2Bytes(listJSON)
	// fmt.Println("Tx Hash is:", tx.Hash())
	// fmt.Println(string(b))

	return list
}

func TrueRWALs(txs []*types.Transaction, chainDB ethdb.Database, sdbBackend statedb.Database, num uint64) []*accesslist.RW_AccessLists {
	fmt.Println("Staring Run True RWALs")
	baseHeadHash := rawdb.ReadCanonicalHash(chainDB, num-1)
	baseHeader := rawdb.ReadHeader(chainDB, baseHeadHash, num-1)

	state, err := statedb.New(baseHeader.Root, sdbBackend, nil)
	if err != nil {
		panic(err)
	}

	headHash := rawdb.ReadCanonicalHash(chainDB, num)
	header := rawdb.ReadHeader(chainDB, headHash, num)

	lists := tracer.CreateRWALWithTransactions(state, txs, header)
	// file, err := os.Create("RWSet")
	// if err != nil {
	// 	fmt.Println("Open file err =", err)
	// 	return
	// }
	// defer file.Close()
	// for id, list := range lists {
	// 	listJSON := list.ToJSON()
	// 	b := common.Hex2Bytes(listJSON)
	// 	_, err := file.WriteString("{" + `"` + txs[id].Hash().String() + `"` + ":" + string(b) + "}\n")
	// 	if err != nil {
	// 		fmt.Println("Write file err =", err)
	// 		return
	// 	}
	// }
	fmt.Println("Finishing Run True RWALs")
	return lists
}

func main() {
	Node, chainDB, sdbBackend := GetEthDatabaseAndStateDatabase()
	defer Node.Close()

	head := rawdb.ReadHeadBlockHash(chainDB)
	num := *rawdb.ReadHeaderNumber(chainDB, head)
	fmt.Println("Block Height:", num)
	headBlock := rawdb.ReadBlock(chainDB, head, num)
	txs := headBlock.Transactions()
	trueLists := TrueRWALs(txs, chainDB, sdbBackend, num)
	// Node.Close()

	// Node, chainDB, sdbBackend = GetEthDatabaseAndStateDatabase()
	predictLists := make([]*accesslist.RW_AccessLists, txs.Len())
	fmt.Println("Staring Run Predicting RWALs")
	for i, tx := range txs {
		fmt.Printf("Starting Predicting Tx[%d]\n", i)
		predictLists[i] = PredictRWAL(tx, chainDB, sdbBackend, num)
	}
	fmt.Println("Finishing Run Predicting RWALs")

	conflictCounter := 0
	nilCounter := 0
	conflictTxs := make([]int, 0)
	for i, list := range trueLists {
		if predictLists[i] == nil {
			nilCounter++
			continue
		}
		if !list.Equal(*predictLists[i]) {
			conflictCounter++
			conflictTxs = append(conflictTxs, i)
		}
	}
	fmt.Println("Nil Prediction Number:", nilCounter)
	fmt.Println("False Prediction Number:", conflictCounter)
	// fmt.Println("False Predicted Transactions:")
	// for _, i := range conflictTxs {
	// 	fmt.Println(txs[i].Hash())
	// 	listJson := predictLists[i].ToJSON()
	// 	b := common.Hex2Bytes(listJson)
	// 	fmt.Println("Predicted RW Sets:", string(b))

	// 	listJson = trueLists[i].ToJSON()
	// 	b = common.Hex2Bytes(listJson)
	// 	fmt.Println("True RW Sets:", string(b))
	// }
	// TODO:预测冲突率、实际冲突率实现

	undiConfGraph := conflictgraph.NewUndirectedGraph()
	for i, tx := range txs {
		undiConfGraph.AddVertex(tx.Hash(), uint(i))
	}

	for i := 0; i < txs.Len(); i++ {
		for j := i + 1; j < txs.Len(); j++ {
			if predictLists[i].HasConflict(*predictLists[j]) {
				undiConfGraph.AddEdge(uint(i), uint(j))
			}
		}
	}

	// graphByte, _ := json.Marshal(undiConfGraph)
	// fmt.Println("Bytelength:", len(graphByte))
	// openFile, _ := os.Create("graph.json")
	// defer openFile.Close()
	// openFile.Write(graphByte)

	groups := undiConfGraph.GetConnectedComponents()
	fmt.Println("Number of Groups:", len(groups))
	for i := 0; i < len(groups); i++ {
		fmt.Printf("Number of Group[%d]:%d\n", i, len(groups[i]))
	}

	for {
		MisSolution := mis.NewSolution(undiConfGraph)
		MisSolution.Solve()
		ansSlice := MisSolution.IndependentSet.ToSlice()
		fmt.Println(len(ansSlice))

		for _, v := range undiConfGraph.Vertices {
			v.IsDeleted = false
			v.Degree = uint(len(undiConfGraph.AdjacencyMap[v.TxId]))
		}
		if len(ansSlice) <= 3 {
			edgeCount := 0
			for id := range undiConfGraph.Vertices {
				edgeCount += len(undiConfGraph.AdjacencyMap[id])
			}
			edgeCount /= 2
			fmt.Println("Node Cound:", len(undiConfGraph.Vertices))
			fmt.Println("Edge Count:", edgeCount)
		}
		for _, v := range ansSlice {
			undiConfGraph.Vertices[v.(uint)].IsDeleted = true
		}
		undiConfGraph = undiConfGraph.CopyGraphWithDeletion()
		if len(undiConfGraph.Vertices) == 0 {
			break
		}
	}
	// 执行交易
	baseHeadHash := rawdb.ReadCanonicalHash(chainDB, num-1)
	baseHeader := rawdb.ReadHeader(chainDB, baseHeadHash, num-1)

	statedb, _ := statedb.New(baseHeader.Root, sdbBackend, nil)
	tracer.ExecuteWithGopool(statedb, predictLists, groups, txs, baseHeader)

}
