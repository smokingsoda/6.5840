package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersionPair struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu    sync.Mutex
	kvMap map[string]ValueVersionPair
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvMap = make(map[string]ValueVersionPair)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	valueAndVersionPair, exists := kv.kvMap[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = valueAndVersionPair.Value
	reply.Version = valueAndVersionPair.Version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	old, exists := kv.kvMap[args.Key]
	if !exists {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
		} else {
			kv.kvMap[args.Key] = ValueVersionPair{args.Value, args.Version + 1}
			reply.Err = rpc.OK
		}
	} else {
		if args.Version != old.Version {
			reply.Err = rpc.ErrVersion
		} else {
			old.Value = args.Value
			old.Version = args.Version + 1
			kv.kvMap[args.Key] = old
			reply.Err = rpc.OK
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
