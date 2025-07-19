package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, leaderId: 0}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{Key: key}
	ms := 50
	i := ck.leaderId
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.leaderId = i
			return reply.Value, reply.Version, reply.Err
		} else if ok && reply.Err == rpc.ErrWrongLeader {
			i = (i + 1) % len(ck.servers)
			continue
		} else if !ok {
			i = (i + 1) % len(ck.servers)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			continue
		} else {
			panic("Get: unreachable")
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	ms := 50
	i := ck.leaderId
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
		if ok && reply.Err == rpc.OK {
			ck.leaderId = i
			return rpc.OK
		} else if ok && reply.Err == rpc.ErrVersion {
			ck.leaderId = i
			return rpc.ErrVersion
		} else if ok && reply.Err == rpc.ErrNoKey {
			ck.leaderId = i
			return rpc.ErrNoKey
		} else if ok && reply.Err == rpc.ErrWrongLeader {
			// Wrong Leader should break too
			// Because what if the leader first appended successfully but lost leader state?
			break
		} else if !ok {
			// Once the network fails, we can't make sure this op applies or not
			break
		} else {
			// log.Printf("ok=%v, err=%v", ok, reply.Err)
			panic("Put: unreachable 1")
		}
	}
	i = (i + 1) % len(ck.servers)
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
		if ok && reply.Err == rpc.OK {
			// This resent args applies, reply OK
			ck.leaderId = i
			return rpc.OK
		} else if ok && reply.Err == rpc.ErrVersion {
			ck.leaderId = i
			return rpc.ErrMaybe
		} else if ok && reply.Err == rpc.ErrNoKey {
			ck.leaderId = i
			return rpc.ErrNoKey
		} else if ok && reply.Err == rpc.ErrWrongLeader {
		} else if !ok {
		} else {
			panic("Put: unreachable 2")
		}
		i = (i + 1) % len(ck.servers)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
