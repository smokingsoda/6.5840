package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	configKey := "config"
	configString := cfg.String()
	err := sck.Put(configKey, configString, 0)
	if err != rpc.OK {
		panic("InitController: fail to init")
	}

}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	currentKey, version, err := sck.Get("config")
	if err != rpc.OK {
		panic("ChangeConfigTo: no key")
	}
	current := shardcfg.FromString(currentKey)
	sck.MoveShard(current, new)
	newString := new.String()
	sck.Put("config", newString, version)
}

func (sck *ShardCtrler) MoveShard(old *shardcfg.ShardConfig, new *shardcfg.ShardConfig) {
	for sh := shardcfg.Tshid(0); sh < shardcfg.NShards; sh++ {
		oldGid := old.Shards[sh]
		newGid := new.Shards[sh]
		if oldGid == newGid {
			continue
		}
		// Need to move from old to new
		oldServers := old.Groups[oldGid]
		newServers := new.Groups[newGid]

		// Make two clerks to make RPC
		oldClk := shardgrp.MakeClerk(sck.clnt, oldServers)
		newClk := shardgrp.MakeClerk(sck.clnt, newServers)

		// Freeze
		state, freezeErr := oldClk.FreezeShard(sh, new.Num)
		if freezeErr != rpc.OK {
			panic("MoveShard: 5A freeze should not")
		}
		// Install
		installErr := newClk.InstallShard(sh, state, new.Num)
		if installErr != rpc.OK {
			panic("MoveShard: 5A install should not")
		}

		// Delete
		deleteErr := oldClk.DeleteShard(sh, new.Num)
		if deleteErr != rpc.OK {
			panic("MoveShard: 5A install should not")
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	configString, _, err := sck.Get("config")
	if err == rpc.ErrNoKey {
		panic("Query: fail to get key")
	}
	cfg := shardcfg.FromString(configString)
	return cfg
}
