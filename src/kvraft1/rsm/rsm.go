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
	pending          map[int]*Op
	lastAppliedIndex int
	count            int
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
		me:               me,
		maxraftstate:     maxraftstate,
		applyCh:          make(chan raftapi.ApplyMsg),
		sm:               sm,
		pending:          make(map[int]*Op),
		lastAppliedIndex: 0,
		count:            0,
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
		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Lock()
			for idx, op := range rsm.pending {
				if idx <= msg.SnapshotIndex {
					delete(rsm.pending, idx)
					close(op.Ch)
				}
			}
			rsm.mu.Unlock()
			rsm.lastAppliedIndex = msg.SnapshotIndex
			continue
		}
		if !msg.CommandValid {
			continue
		}
		req := msg.Command
		// DoOp first
		result := rsm.sm.DoOp(req)
		rsm.lastAppliedIndex = msg.CommandIndex
		// Need to check if raft still in the term and leader state when command submited
		rsm.mu.Lock()
		op, exist := rsm.pending[msg.CommandIndex]
		if exist {
			// The command was executed in the same term *and* we're still leader –
			// deliver the result back to the original caller.
			delete(rsm.pending, msg.CommandIndex)
			op.Ch <- result
		}
		rsm.mu.Unlock()

		if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
			data := rsm.sm.Snapshot()
			rsm.rf.Snapshot(rsm.lastAppliedIndex, data)
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
	if rsm.rf == nil {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	rsm.mu.Lock()
	// Store the chan in the map
	if rsm.applyCh == nil {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	originIndex, originTerm, isLeader := rsm.rf.Start(req)

	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan any, 1)
	op := Op{Me: rsm.me, Index: originIndex, Term: originTerm, Req: req, Ch: ch}
	if oldOp, exist := rsm.pending[originIndex]; exist {
		delete(rsm.pending, originIndex)
		close(oldOp.Ch)
	}
	rsm.pending[originIndex] = &op
	rsm.mu.Unlock()
	// Wait for the command to either be applied (ApplyChReader will send the
	// result on ch) *or* for this server to step down as leader (term change
	// or leadership lost). Use a small ticker to periodically re-check the
	// leadership state so that Submit eventually returns instead of blocking
	// forever when the server loses leadership without the command being
	// committed (e.g., in a minority partition).
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case result, ok := <-ch:
			// ApplyChReader delivered a result or the channel has been closed.
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			currentTerm, currentIsLeader := rsm.rf.GetState()
			if !currentIsLeader || currentTerm != originTerm {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, result

		case <-ticker.C:
			currentTerm, currentIsLeader := rsm.rf.GetState()
			if !currentIsLeader || currentTerm != originTerm {
				// No longer leader – clean up and tell caller to retry.
				rsm.mu.Lock()
				if opCur, exist := rsm.pending[originIndex]; exist && opCur.Ch == ch {
					delete(rsm.pending, originIndex)
					close(ch)
				}
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Lock()
			if rsm.applyCh == nil {
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
		}
	}
}
