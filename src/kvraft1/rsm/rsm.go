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
	pending map[int]chan any
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
		// index:        1,
		// term:         0,
		pending: make(map[int]chan any),
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
	Debug(dApply, "S%d: ApplyChReader started", rsm.me)
	for {
		msg, ok := <-rsm.applyCh
		if !ok {
			Debug(dApply, "S%d: ApplyChReader channel closed, cleaning up pending requests", rsm.me)
			rsm.mu.Lock()
			for _, ch := range rsm.pending {
				close(ch)
			}
			rsm.applyCh = nil
			rsm.mu.Unlock()
			Debug(dApply, "S%d: ApplyChReader exiting", rsm.me)
			return
		}
		if !msg.CommandValid {
			Debug(dApply, "S%d: Received invalid command message, skipping", rsm.me)
			continue
		}
		op := msg.Command.(Op)
		Debug(dApply, "S%d: Applying op from S%d, timestamp=%d, req=%v", rsm.me, op.Me, op.TimeStamp, op.Req)

		rsm.mu.Lock()
		ch, exist := rsm.pending[op.TimeStamp]
		if exist {
			Debug(dPending, "S%d: Found pending request for timestamp=%d", rsm.me, op.TimeStamp)
			delete(rsm.pending, op.TimeStamp)
		} else {
			Debug(dPending, "S%d: No pending request found for timestamp=%d", rsm.me, op.TimeStamp)
		}
		rsm.mu.Unlock()

		if exist && op.Me == rsm.me {
			// leader, return by submit goroutine
			Debug(dApply, "S%d: Leader applying op, sending result to submit goroutine", rsm.me)
			result := rsm.sm.DoOp(op.Req)
			Debug(dState, "S%d: DoOp result=%v", rsm.me, result)
			ch <- result
		} else {
			// follower, return by this goroutine
			Debug(dApply, "S%d: Follower applying op", rsm.me)
			result := rsm.sm.DoOp(op.Req)
			Debug(dState, "S%d: DoOp result=%v", rsm.me, result)
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

	// Make timestamp is unique
	rsm.mu.Lock()
	op := Op{rsm.me, int(time.Now().UnixNano()), req}
	// Store the chan in the map
	ch := make(chan any, 1)
	rsm.pending[op.TimeStamp] = ch
	Debug(dSubmit, "S%d: Submitting op with timestamp=%d, req=%v", rsm.me, op.TimeStamp, req)
	rsm.mu.Unlock()

	_, originTerm, isLeader := rsm.rf.Start(op)
	Debug(dSubmit, "S%d: Raft.Start returned term=%d, isLeader=%v", rsm.me, originTerm, isLeader)

	if !isLeader {
		Debug(dSubmit, "S%d: Not leader, cleaning up pending request", rsm.me)
		rsm.mu.Lock()
		delete(rsm.pending, op.TimeStamp)
		Debug(dPending, "S%d: Pending map size after cleanup: %d", rsm.me, len(rsm.pending))
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	Debug(dSubmit, "S%d: Waiting for op to be applied, timestamp=%d", rsm.me, op.TimeStamp)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case result, ok := <-ch:
			if !ok {
				Debug(dSubmit, "S%d: Channel closed, returning ErrWrongLeader", rsm.me)
				return rpc.ErrWrongLeader, nil
			}
			Debug(dSubmit, "S%d: Op applied successfully, returning result", rsm.me)
			return rpc.OK, result
		case <-ticker.C:
			term, isLeader := rsm.rf.GetState()
			rsm.mu.Lock()
			if originTerm != term || !isLeader || rsm.applyCh == nil {
				Debug(dSubmit, "S%d: Leadership changed or channel closed, term=%d->%d, isLeader=%v", rsm.me, originTerm, term, isLeader)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
		}
	}
}
