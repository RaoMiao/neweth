// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package eth implements the Ethereum protocol.
package eth

import (
	"fmt"
	"math/big"
	"runtime"
	"sync"

	"neweth/common"
	"neweth/miner"

	"neweth/consensus"
	"neweth/consensus/ethash"
	"neweth/core"
	"neweth/core/rawdb"
	"neweth/core/types"
	"neweth/core/vm"
	"neweth/ethdb"
	"neweth/event"

	//"github.com/ethereum/go-ethereum/accounts"

	//"github.com/ethereum/go-ethereum/internal/ethapi"
	"neweth/node"
	"neweth/params"
	"neweth/rlp"
)

// Ethereum implements the Ethereum full node service.
type Ethereum struct {
	config      *Config
	chainConfig *params.ChainConfig

	// Channel for shutting down the service
	shutdownChan chan bool // Channel for shutting down the Ethereum

	// Handlers
	txPool     *core.TxPool
	blockchain *core.BlockChain
	//protocolManager *ProtocolManager
	//lesServer LesServer

	// DB interfaces
	chainDb ethdb.Database // Block chain database

	eventMux *event.TypeMux
	engine   consensus.Engine
	//accountManager *accounts.Manager

	//APIBackend *EthAPIBackend

	miner     *miner.Miner
	gasPrice  *big.Int
	etherbase common.Address

	networkID uint64
	//netRPCService *ethapi.PublicNetAPI

	lock sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
}

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(ctx *node.ServiceContext, config *Config) (*Ethereum, error) {
	chainDb, err := CreateDB(ctx, config, "chaindata")
	if err != nil {
		return nil, err
	}
	chainConfig, genesisHash, genesisErr := core.SetupGenesisBlock(chainDb, config.Genesis)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}
	//log.Info("Initialised chain configuration", "config", chainConfig)

	eth := &Ethereum{
		config:      config,
		chainDb:     chainDb,
		chainConfig: chainConfig,
		eventMux:    ctx.EventMux,
		//accountManager: ctx.AccountManager,
		engine:       CreateConsensusEngine(ctx, &config.Ethash, chainConfig, chainDb),
		shutdownChan: make(chan bool),
		networkID:    config.NetworkId,
		gasPrice:     config.GasPrice,
		etherbase:    config.Etherbase,
		// bloomRequests: make(chan chan *bloombits.Retrieval),
		//bloomIndexer:   NewBloomIndexer(chainDb, params.BloomBitsBlocks),
	}

	//log.Info("Initialising Ethereum protocol", "versions", ProtocolVersions, "network", config.NetworkId)

	if !config.SkipBcVersionCheck {
		bcVersion := rawdb.ReadDatabaseVersion(chainDb)
		if bcVersion != core.BlockChainVersion && bcVersion != 0 {
			return nil, fmt.Errorf("Blockchain DB version mismatch (%d / %d). Run geth upgradedb.\n", bcVersion, core.BlockChainVersion)
		}
		rawdb.WriteDatabaseVersion(chainDb, core.BlockChainVersion)
	}
	var (
		vmConfig    = vm.Config{EnablePreimageRecording: config.EnablePreimageRecording}
		cacheConfig = &core.CacheConfig{Disabled: config.NoPruning, TrieNodeLimit: config.TrieCache, TrieTimeLimit: config.TrieTimeout}
	)
	eth.blockchain, err = core.NewBlockChain(chainDb, cacheConfig, eth.chainConfig, eth.engine, vmConfig)
	if err != nil {
		return nil, err
	}
	// Rewind the chain in case of an incompatible config upgrade.
	if compat, ok := genesisErr.(*params.ConfigCompatError); ok {
		//log.Warn("Rewinding chain to upgrade configuration", "err", compat)
		eth.blockchain.SetHead(compat.RewindTo)
		rawdb.WriteChainConfig(chainDb, genesisHash, chainConfig)
	}
	//eth.bloomIndexer.Start(eth.blockchain)

	if config.TxPool.Journal != "" {
		config.TxPool.Journal = ctx.ResolvePath(config.TxPool.Journal)
	}
	eth.txPool = core.NewTxPool(config.TxPool, eth.chainConfig, eth.blockchain)

	// if eth.protocolManager, err = NewProtocolManager(eth.chainConfig, config.SyncMode, config.NetworkId, eth.eventMux, eth.txPool, eth.engine, eth.blockchain, chainDb); err != nil {
	// 	return nil, err
	// }
	eth.miner = miner.New(eth, eth.chainConfig, eth.EventMux(), eth.engine)
	eth.miner.SetExtra(makeExtraData(config.ExtraData))

	// eth.APIBackend = &EthAPIBackend{eth, nil}
	// gpoParams := config.GPO
	// if gpoParams.Default == nil {
	// 	gpoParams.Default = config.GasPrice
	// }
	// eth.APIBackend.gpo = gasprice.NewOracle(eth.APIBackend, gpoParams)

	return eth, nil
}

func makeExtraData(extra []byte) []byte {
	if len(extra) == 0 {
		// create default extradata
		extra, _ = rlp.EncodeToBytes([]interface{}{
			uint(params.VersionMajor<<16 | params.VersionMinor<<8 | params.VersionPatch),
			"geth",
			runtime.Version(),
			runtime.GOOS,
		})
	}
	if uint64(len(extra)) > params.MaximumExtraDataSize {
		//log.Warn("Miner extra data exceed limit", "extra", hexutil.Bytes(extra), "limit", params.MaximumExtraDataSize)
		extra = nil
	}
	return extra
}

// 创建区块链数据库
// CreateDB creates the chain database.
func CreateDB(ctx *node.ServiceContext, config *Config, name string) (ethdb.Database, error) {
	db, err := ctx.OpenDatabase(name, config.DatabaseCache, config.DatabaseHandles)
	if err != nil {
		return nil, err
	}
	if db, ok := db.(*ethdb.LDBDatabase); ok {
		db.Meter("eth/db/chaindata/")
	}
	return db, nil
}

// 共识引擎的创建
// CreateConsensusEngine creates the required type of consensus engine instance for an Ethereum service
func CreateConsensusEngine(ctx *node.ServiceContext, config *ethash.Config, chainConfig *params.ChainConfig, db ethdb.Database) consensus.Engine {
	// If proof-of-authority is requested, set it up

	// Otherwise assume proof-of-work
	engine := ethash.New(ethash.Config{
		CacheDir:       ctx.ResolvePath(config.CacheDir),
		CachesInMem:    config.CachesInMem,
		CachesOnDisk:   config.CachesOnDisk,
		DatasetDir:     config.DatasetDir,
		DatasetsInMem:  config.DatasetsInMem,
		DatasetsOnDisk: config.DatasetsOnDisk,
	})
	engine.SetThreads(-1) // Disable CPU mining
	return engine

}

func (s *Ethereum) ResetWithGenesisBlock(gb *types.Block) {
	s.blockchain.ResetWithGenesisBlock(gb)
}

func (s *Ethereum) BlockChain() *core.BlockChain { return s.blockchain }
func (s *Ethereum) TxPool() *core.TxPool         { return s.txPool }
func (s *Ethereum) EventMux() *event.TypeMux     { return s.eventMux }
func (s *Ethereum) Engine() consensus.Engine     { return s.engine }
func (s *Ethereum) ChainDb() ethdb.Database      { return s.chainDb }
func (s *Ethereum) IsListening() bool            { return true } // Always listening
func (s *Ethereum) NetVersion() uint64           { return s.networkID }

// Start implements node.Service, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *Ethereum) Start() error {

	// Figure out a max peers count based on the server limits

	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *Ethereum) Stop() error {
	//s.bloomIndexer.Close()
	s.blockchain.Stop()

	s.txPool.Stop()
	//s.miner.Stop()
	s.eventMux.Stop()

	s.chainDb.Close()
	close(s.shutdownChan)

	return nil
}

func (s *Ethereum) Etherbase() (eb common.Address, err error) {
	s.lock.RLock()
	etherbase := s.etherbase
	s.lock.RUnlock()

	if etherbase != (common.Address{}) {
		return etherbase, nil
	}
	return common.Address{}, fmt.Errorf("etherbase must be explicitly specified")
}

//挖矿相关
func (s *Ethereum) StartMining(local bool) error {
	eb, err := s.Etherbase()
	if err != nil {

		return fmt.Errorf("etherbase missing: %v", err)
	}

	//NEED DO!!这是干嘛的呢
	// if local {
	// 	// If local (CPU) mining is started, we can disable the transaction rejection
	// 	// mechanism introduced to speed sync times. CPU mining on mainnet is ludicrous
	// 	// so none will ever hit this path, whereas marking sync done on CPU mining
	// 	// will ensure that private networks work in single miner mode too.
	// 	atomic.StoreUint32(&s.protocolManager.acceptTxs, 1)
	// }
	go s.miner.Start(eb)
	return nil
}
