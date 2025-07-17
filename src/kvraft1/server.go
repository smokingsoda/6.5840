package kvraft

import (
	"log"
	"sync"
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
	mu    sync.Mutex
	kvMap map[string]ValueVersionPair
	locks map[string]*sync.Mutex
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

	switch r := req.(type) {
	case rpc.GetArgs:
		{
			// log.Printf("S%d DoOp=%v", kv.me, r)
			reply := rpc.GetReply{}
			if kv.killed() {
				reply.Err = rpc.ErrWrongLeader
				return reply
			}
			// lock := kv.getLock(r.Key)
			// lock.Lock()
			// defer lock.Unlock()
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
			// log.Printf("S%d DoOp=%v", kv.me, r)
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
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
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
	kv.locks = make(map[string]*sync.Mutex)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
