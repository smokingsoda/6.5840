package kvraft

import (
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	// mu    sync.Mutex
	kvMap map[string]ValueVersionPair
}

type ValueVersionPair struct {
	Value   string
	Version rpc.Tversion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	get, ok := req.(rpc.GetArgs)
	if ok {
		reply := rpc.GetReply{}
		valueAndVersionPair, exists := kv.kvMap[get.Key]
		if !exists {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		reply.Value = valueAndVersionPair.Value
		reply.Version = valueAndVersionPair.Version
		reply.Err = rpc.OK
		return reply
	} else {
		put, ok := req.(rpc.PutArgs)
		if !ok {
			panic("DoOp: Unexpected type")
		}
		reply := rpc.PutReply{}
		old, exists := kv.kvMap[put.Key]
		if !exists {
			if put.Version != 0 {
				reply.Err = rpc.ErrNoKey
			} else {
				kv.kvMap[put.Key] = ValueVersionPair{put.Value, put.Version + 1}
				reply.Err = rpc.OK
			}
		} else {
			if put.Version != old.Version {
				reply.Err = rpc.ErrVersion
			} else {
				old.Value = put.Value
				old.Version = put.Version + 1
				kv.kvMap[put.Key] = old
				reply.Err = rpc.OK
			}
		}
		return reply
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	req := *args
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = result.(rpc.GetReply)
	return
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	req := *args
	err, result := kv.rsm.Submit(req)
	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = result.(rpc.PutReply)
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.kvMap = make(map[string]ValueVersionPair)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
