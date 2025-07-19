package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"

	tester "6.5840/tester1"
)

type ShardInfo struct {
	ConfigNum shardcfg.Tnum
	Frozen    bool
	Owned     bool
}
type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu         sync.Mutex
	kvMap      map[string]ValueVersionPair
	shardInfos [shardcfg.NShards]ShardInfo
}

type ValueVersionPair struct {
	Value   string
	Version rpc.Tversion
}

func (kv *KVServer) serializeShardData(shard shardcfg.Tshid) []byte {
	shardData := make(map[string]ValueVersionPair)
	for key, pair := range kv.kvMap {
		if shardcfg.Key2Shard(key) == shard {
			shardData[key] = pair
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shardData)
	return w.Bytes()
}

func (kv *KVServer) deserializeShardData(data []byte) map[string]ValueVersionPair {
	if len(data) == 0 {
		return make(map[string]ValueVersionPair)
	}
	r := bytes.NewBuffer(data)
	e := labgob.NewDecoder(r)
	var shardData map[string]ValueVersionPair
	if e.Decode(&shardData) != nil {
		panic("failed to decode shard data")
	}
	return shardData
}

func (kv *KVServer) ShardOwned(sh shardcfg.Tshid) bool {
	if sh < 0 || sh >= shardcfg.NShards {
		return false
	}
	return kv.shardInfos[sh].Owned
}

func (kv *KVServer) ShardFrozen(sh shardcfg.Tshid) bool {
	if sh < 0 || sh >= shardcfg.NShards {
		return false
	}
	return kv.shardInfos[sh].Frozen
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch r := req.(type) {
	case rpc.GetArgs:
		{
			reply := rpc.GetReply{}
			if kv.killed() {
				reply.Err = rpc.ErrWrongLeader
				return reply
			}
			shard := shardcfg.Key2Shard(r.Key)
			if !kv.ShardOwned(shard) {
				reply.Err = rpc.ErrWrongGroup
				return reply
			}
			if kv.ShardFrozen(shard) {
				reply.Err = rpc.ErrWrongGroup
				return reply
			}
			valueAndVersionPair, exists := kv.kvMap[r.Key]
			if !exists {
				reply.Err = rpc.ErrNoKey
				return reply
			}
			reply.Value = valueAndVersionPair.Value
			reply.Version = valueAndVersionPair.Version
			reply.Err = rpc.OK
			return reply
		}
	case rpc.PutArgs:
		{
			reply := rpc.PutReply{}
			if kv.killed() {
				reply.Err = rpc.ErrWrongLeader
				return reply
			}
			shard := shardcfg.Key2Shard(r.Key)
			if !kv.ShardOwned(shard) {
				reply.Err = rpc.ErrWrongGroup
				return reply
			}
			if kv.ShardFrozen(shard) {
				reply.Err = rpc.ErrWrongGroup
				return reply
			}
			old, exists := kv.kvMap[r.Key]
			if !exists {
				if r.Version != 0 {
					reply.Err = rpc.ErrNoKey
				} else {
					kv.kvMap[r.Key] = ValueVersionPair{r.Value, r.Version + 1}
					reply.Err = rpc.OK
				}
			} else {
				if r.Version != old.Version {
					reply.Err = rpc.ErrVersion
				} else {
					kv.kvMap[r.Key] = ValueVersionPair{Value: r.Value, Version: r.Version + 1}
					reply.Err = rpc.OK
				}
			}
			return reply
		}
	case shardrpc.FreezeShardArgs:
		reply := shardrpc.FreezeShardReply{}
		if kv.killed() {
			reply.Err = rpc.ErrWrongLeader
			return reply
		}
		configNum := kv.shardInfos[r.Shard].ConfigNum
		if r.Num <= configNum {
			// Out of date num, applied before
			reply.Err = rpc.OK
			reply.State = kv.serializeShardData(r.Shard)
			reply.Num = configNum
			return reply
		}

		if !kv.ShardOwned(r.Shard) {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		if !kv.ShardFrozen(r.Shard) {
			kv.shardInfos[r.Shard].Frozen = true
		}

		kv.shardInfos[r.Shard].ConfigNum = r.Num

		reply.Err = rpc.OK
		reply.State = kv.serializeShardData(r.Shard)
		reply.Num = configNum
		return reply

	case shardrpc.InstallShardArgs:
		reply := shardrpc.InstallShardReply{}
		if kv.killed() {
			reply.Err = rpc.ErrWrongLeader
			return reply
		}
		configNum := kv.shardInfos[r.Shard].ConfigNum
		if r.Num <= configNum {
			// Out of date num, applied before
			reply.Err = rpc.OK
			return reply
		}
		installData := kv.deserializeShardData(r.State)
		for key, valueVersion := range installData {
			// Should accept in all cases, because configNum is the absolute indicatior
			kv.kvMap[key] = valueVersion
		}
		kv.shardInfos[r.Shard].ConfigNum = r.Num
		kv.shardInfos[r.Shard].Frozen = false
		kv.shardInfos[r.Shard].Owned = true

		reply.Err = rpc.OK
		return reply
	case shardrpc.DeleteShardArgs:
		reply := shardrpc.DeleteShardReply{}
		if kv.killed() {
			reply.Err = rpc.ErrWrongLeader
			return reply
		}
		configNum := kv.shardInfos[r.Shard].ConfigNum
		if r.Num <= configNum {
			// Out of date num, applied before
			reply.Err = rpc.OK
			return reply
		}

		if !kv.ShardOwned(r.Shard) {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		deleteList := make([]string, 0)
		for key, _ := range kv.kvMap {
			if shardcfg.Key2Shard(key) == r.Shard {
				deleteList = append(deleteList, key)
			}
		}
		for i := range deleteList {
			delete(kv.kvMap, deleteList[i])
		}
		kv.shardInfos[r.Shard].ConfigNum = r.Num
		kv.shardInfos[r.Shard].Frozen = false
		kv.shardInfos[r.Shard].Owned = false
		reply.Err = rpc.OK
		return reply

	default:
		{
			panic("DoOp: impossible")
		}
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.shardInfos)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if data == nil || len(data) == 0 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var m map[string]ValueVersionPair = make(map[string]ValueVersionPair)
	var s [shardcfg.NShards]ShardInfo
	if d.Decode(&m) != nil || d.Decode(&s) != nil {
		log.Fatalf("decode snapshot")
	}
	kv.kvMap = m
	kv.shardInfos = s
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	req := *args
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	getReply, ok := result.(rpc.GetReply)
	if !ok {
		log.Printf("Get: actual type is %T", result)
		panic("Get: impossible")
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = getReply
	return
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	req := *args
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	putReply, ok := result.(rpc.PutReply)
	if !ok {
		log.Printf("Put: actual type is %T", result)
		panic("Put: impossible")
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = putReply
	return
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	req := *args
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	freezeReply, ok := result.(shardrpc.FreezeShardReply)
	if !ok {
		log.Printf("FreezeShard: actual type is %T", result)
		panic("FreezeShard: impossible")
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = freezeReply
	return
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	req := *args
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	installReply, ok := result.(shardrpc.InstallShardReply)
	if !ok {
		log.Printf("InstallShard: actual type is %T", result)
		panic("InstallShard: impossible")
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = installReply
	return
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	req := *args
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	deleteReply, ok := result.(shardrpc.DeleteShardReply)
	if !ok {
		log.Printf("DeleteShard: actual type is %T", result)
		panic("DeleteShard: impossible")
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = deleteReply
	return
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me, shardInfos: [shardcfg.NShards]ShardInfo{}}

	kv.kvMap = make(map[string]ValueVersionPair)

	if gid == shardcfg.Gid1 {
		for i := range kv.shardInfos {
			kv.shardInfos[i].ConfigNum = shardcfg.NumFirst
			kv.shardInfos[i].Owned = true
			kv.shardInfos[i].Frozen = false
		}
	} else {
		for i := range kv.shardInfos {
			kv.shardInfos[i].ConfigNum = shardcfg.NumFirst - 1
			kv.shardInfos[i].Owned = false
			kv.shardInfos[i].Frozen = false
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
