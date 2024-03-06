package utils

import (
	"interact/accesslist"
	conflictgraph "interact/conflictGraph"
	"sort"

	"github.com/ethereum/go-ethereum/core/types"
)

func GenerateCCGroups(vertexGroup [][]*conflictgraph.Vertex, txs types.Transactions, predictRWSets accesslist.RWSetList) ([]types.Transactions, []accesslist.RWSetList) {
	txsGroup := make([]types.Transactions, len(vertexGroup))
	RWSetsGroup := make([]accesslist.RWSetList, len(vertexGroup))
	for i := 0; i < len(vertexGroup); i++ {
		sort.Slice(vertexGroup[i], func(j, k int) bool {
			return vertexGroup[i][j].TxId < vertexGroup[i][k].TxId
		})

		for j := 0; j < len(vertexGroup[i]); j++ {
			txsGroup[i] = append(txsGroup[i], txs[vertexGroup[i][j].TxId])
			RWSetsGroup[i] = append(RWSetsGroup[i], predictRWSets[vertexGroup[i][j].TxId])
		}
	}
	return txsGroup, RWSetsGroup
}
