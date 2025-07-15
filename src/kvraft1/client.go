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
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, leaderId: 0}
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
	ms := 100
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
	putArgs := &rpc.PutArgs{Key: key, Value: value, Version: version}
	firstAttempt := true
	serverId := ck.leaderId
	for {
		for i := 0; i < len(ck.servers); i++ {
			server := ck.servers[serverId]
			var putReply rpc.PutReply
			if ok := ck.clnt.Call(server, "KVServer.Put", putArgs, &putReply); ok {
				if putReply.Err == rpc.OK {
					ck.leaderId = serverId
					return rpc.OK // 成功更新键值对
				} else if putReply.Err == rpc.ErrNoKey {
					ck.leaderId = serverId
					return rpc.ErrNoKey // 键不存在
				} else if putReply.Err == rpc.ErrVersion {
					ck.leaderId = serverId
					if firstAttempt {
						return rpc.ErrVersion // 第一次尝试收到 ErrVersion，直接返回
					} else {
						return rpc.ErrMaybe // 重试阶段收到 ErrVersion，返回 ErrMaybe
					}
				} else if putReply.Err == rpc.ErrWrongLeader {
					// 如果是 ErrWrongLeader，继续尝试下一个服务器
				}
			}
			firstAttempt = false // 之后都是重试阶段
			// 执行到这说明，RPC调用失败或者返回了 ErrWrongLeader
			serverId = (serverId + 1) % len(ck.servers) // 循环尝试下一个服务器
		}
		time.Sleep(500 * time.Millisecond) // 等待500ms再重试
	}
}
