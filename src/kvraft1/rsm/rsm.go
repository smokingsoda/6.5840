package rsm

import (
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
	Me    int
	Index int
	Term  int
	Req   any
	Ch    chan any
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
	pending map[int]*Op
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
		pending:      make(map[int]*Op),
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
		// close by raft.Kill()
		if !ok {
			rsm.mu.Lock()
			for _, op := range rsm.pending {
				close(op.Ch)
			}
			rsm.applyCh = nil
			rsm.pending = make(map[int]*Op)
			rsm.mu.Unlock()
			return
		}
		if !msg.CommandValid {
			continue
		}

		req := msg.Command

		// DoOp first
		result := rsm.sm.DoOp(req)
		// Need to check if raft still in the term and leader state when command submited
		term, isLeader := rsm.rf.GetState()

		rsm.mu.Lock()
		op, exist := rsm.pending[msg.CommandIndex]
		if exist && term == op.Term && isLeader {
			// The command was executed in the same term *and* we're still leader –
			// deliver the result back to the original caller.
			delete(rsm.pending, msg.CommandIndex)
			op.Ch <- result
		} else if exist {
			// Raft term or leadership has changed since the command was submitted.
			// All outstanding requests that were waiting for the old term/leader
			// must be failed so their Submit() invocations can return and the
			// goroutines can exit.  Close every pending channel and reset the map.
			for idx, p := range rsm.pending {
				close(p.Ch)
				delete(rsm.pending, idx)
			}
		}
		rsm.mu.Unlock()
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	originIndex, originTerm, isLeader := rsm.rf.Start(req)
	if !isLeader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	// Make timestamp is unique
	rsm.mu.Lock()
	// Store the chan in the map
	if rsm.applyCh == nil {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	ch := make(chan any, 1)
	op := Op{Me: rsm.me, Index: originIndex, Term: originTerm, Req: req, Ch: ch}
	rsm.pending[originIndex] = &op
	rsm.mu.Unlock()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case result, ok := <-ch:
			// close by ApplyChReader
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, result
		case <-ticker.C:
			// check regularly, and this index is useless, maybe rewritten by other submut goroutine
			// should not delete in this goroutine
			term, isLeader := rsm.rf.GetState()
			rsm.mu.Lock()
			if originTerm != term || !isLeader || rsm.applyCh == nil {
				if op.Index != originIndex {
					panic("Submit: impossible")
				}
				delete(rsm.pending, op.Index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
		}
	}
}
