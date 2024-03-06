package testfunc

import (
	"fmt"
	"interact/core"
	fullstate "interact/state"
	"interact/tracer"
	"interact/utils"

	"github.com/ethereum/go-ethereum/core/rawdb"
	statedb "github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/ethdb"
)

const PREDICTBLOCKNUMBER = 100

// use one statedb to predict ten block's transactions
func WarmBlocks(chainDB ethdb.Database, sdbBackend statedb.Database, startNum uint64) *statedb.StateDB {
	// get a state db before ten blocks
	baseHeadHash := rawdb.ReadCanonicalHash(chainDB, startNum-1)
	baseHeader := rawdb.ReadHeader(chainDB, baseHeadHash, startNum-1)

	state, err := statedb.New(baseHeader.Root, sdbBackend, nil)
	if err != nil {
		panic(err)
	}
	fulldb := fullstate.NewStateWithRwSets(state)

	fakeChainCtx := core.NewFakeChainContext(chainDB)

	var i uint64
	for i = 0; i < PREDICTBLOCKNUMBER; i++ {
		block, header := utils.GetBlockAndHeader(chainDB, startNum+i)
		txs := block.Transactions()
		fmt.Println("blocknum:", block.Number())
		for _, tx := range txs {
			tracer.ExecToGenerateRWSet(fulldb, tx, header, fakeChainCtx)
		}
	}

	return state
}
