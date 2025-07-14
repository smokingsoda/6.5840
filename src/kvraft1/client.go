package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	i := 0
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Value, reply.Version, reply.Err
		}
		<-ticker.C
		i = (i + 1) % len(ck.servers)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	i := 0
	ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
	if ok && reply.Err != rpc.ErrWrongLeader {
		return reply.Err
	}
	for {
		i = (i + 1) % len(ck.servers)
		ok = ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
		if ok && reply.Err == rpc.OK {
			// This resent args applies, reply OK
			return reply.Err
		}
		if ok && reply.Err != rpc.ErrWrongLeader {
			// Once it is wrong leader, whether what we sent before has definitely discrad due to raft
			return rpc.ErrMaybe
		}
		<-ticker.C
	}
}
