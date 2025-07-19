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
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu    sync.Mutex
	kvMap map[string]ValueVersionPair
}

type ValueVersionPair struct {
	Value   string
	Version rpc.Tversion
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
	if err := e.Encode(kv.kvMap); err != nil {
		log.Fatalf("encode snapshot: %v", err)
	}
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
	if err := d.Decode(&m); err != nil {
		log.Fatalf("decode snapshot: %v", err)
	}
	kv.kvMap = m
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
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
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

	kv := &KVServer{gid: gid, me: me}

	kv.kvMap = make(map[string]ValueVersionPair)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
