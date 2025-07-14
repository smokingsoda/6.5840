package rsm

import (
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me        int
	TimeStamp int
	Req       any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	index   int
	pending map[int]chan raftapi.ApplyMsg
	term    int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		index:        1,
		term:         0,
		pending:      make(map[int]chan raftapi.ApplyMsg),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
		if rsm.rf == nil {
			panic("raft.Make returned nil")
		}
	} else {
		panic("useRaftStateMachine is true, but no alternative raft implementation is provided")
	}
	go rsm.ApplyChReader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) ApplyChReader() {
	for {
		msg, ok := <-rsm.applyCh
		if !ok {
			rsm.mu.Lock()
			for timeStamp, ch := range rsm.pending {
				close(ch)
				delete(rsm.pending, timeStamp)
			}
			rsm.applyCh = nil
			rsm.mu.Unlock()
			return
		}
		if !msg.CommandValid {
			continue
		}
		rsm.mu.Lock()
		if msg.CommandIndex == rsm.index {
			rsm.index = msg.CommandIndex + 1
		} else if msg.CommandIndex > rsm.index {
			panic("ApplyChReader: impossible")
		} else if msg.CommandIndex < rsm.index {
			// duplicate apply, ignore
			rsm.mu.Unlock()
			continue
		}
		op := msg.Command.(Op)
		ch, exist := rsm.pending[op.TimeStamp]
		if exist && op.Me == rsm.me {
			// leader, return by submit goroutine
			delete(rsm.pending, op.TimeStamp)
			rsm.mu.Unlock()
			ch <- msg
		} else {
			// follower, return by this goroutine
			rsm.sm.DoOp(op.Req)
			rsm.mu.Unlock()
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	rsm.mu.Lock()
	if rsm.rf == nil || rsm.applyCh == nil {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	op := Op{rsm.me, int(time.Now().UnixNano()), req}
	_, term, isLeader := rsm.rf.Start(op)
	log.Printf("S%d, submit %d starts", rsm.me, op.TimeStamp)
	defer log.Printf("S%d, submit %d done", rsm.me, op.TimeStamp)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	// Store the chan in the map
	ch := make(chan raftapi.ApplyMsg)
	rsm.pending[op.TimeStamp] = ch
	rsm.term = term
	rsm.mu.Unlock()
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			if !msg.CommandValid {
				return rpc.ErrWrongLeader, nil
			}
			returnOp := msg.Command.(Op)
			rsm.mu.Lock()
			if op != returnOp {
				panic("Submit: impossible")
			} else {
				result := rsm.sm.DoOp(returnOp.Req)
				rsm.mu.Unlock()
				return rpc.OK, result
			}
		case <-ticker.C:
			rsm.mu.Lock()
			term, _ = rsm.rf.GetState()
			if rsm.term != term {
				delete(rsm.pending, op.TimeStamp)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
		}
	}
}
