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
	Me  int
	Id  int
	Req any
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
	id      int
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
		id:           0,
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
			return
		}
		if !msg.CommandValid {
			continue
		}
		// log.Printf("S%d, receive msg index %d", rsm.me, msg.CommandIndex)
		rsm.mu.Lock()
		if msg.CommandIndex == rsm.index {
			rsm.index = msg.CommandIndex + 1
			// log.Printf("S%d, increment index to %d, value %v", rsm.me, rsm.index, msg.Command)
		} else if msg.CommandIndex > rsm.index {
			panic("ApplyChReader: impossible")
		} else if msg.CommandIndex < rsm.index {
			// duplicate apply, ignore
			// log.Printf("S%d, reply %d", rsm.me, msg.CommandIndex)
			rsm.mu.Unlock()
			continue
		}
		op := msg.Command.(Op)
		ch, exist := rsm.pending[op.Id]
		if exist && op.Me == rsm.me {
			// leader, return by submit goroutine
			delete(rsm.pending, op.Id)
			rsm.mu.Unlock()
			ch <- msg
		} else {
			// follower, return by this goroutine
			// log.Printf("CCCS%d, submit, index %d, value %v", rsm.me, rsm.index-1, msg.Command)
			rsm.sm.DoOp(op.Req)
			rsm.mu.Unlock()
		}
	}
}

func (rsm *RSM) SubmitCleaner() {
	term, _ := rsm.rf.GetState()
	if term == rsm.term {
		return
	}
	rsm.term = term
	for id, ch := range rsm.pending {
		if id < rsm.id {
			delete(rsm.pending, id)
			msg := raftapi.ApplyMsg{CommandValid: false}
			ch <- msg
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
	if rsm.rf == nil {
		rsm.mu.Unlock()
		// log.Printf("S%d, submit return A", rsm.me)
		return rpc.ErrWrongLeader, nil
	}
	op := Op{rsm.me, rsm.id, req}
	_, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		// It is not the leader, we can't submit by this server
		// log.Printf("S%d, not leader", rsm.me)
		rsm.SubmitCleaner()
		rsm.mu.Unlock()
		// log.Printf("S%d, submit return B", rsm.me)
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	// Store the chan in the map
	ch := make(chan raftapi.ApplyMsg)
	// log.Printf("S%d, put id %d value %v", rsm.me, op.Id, op)
	rsm.pending[op.Id] = ch
	if rsm.term != term {
		rsm.SubmitCleaner()
	}
	rsm.term = term
	// Increment the id
	rsm.id += 1
	rsm.mu.Unlock()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case msg := <-ch:
			if !msg.CommandValid {
				// log.Printf("S%d, submit return C", rsm.me)
				return rpc.ErrWrongLeader, nil
			}
			returnOp := msg.Command.(Op)
			rsm.mu.Lock()
			if op != returnOp {
				// log.Printf("AAAS%d, submit, index %d, value %v", rsm.me, rsm.index-1, msg.Command)
				// rsm.sm.DoOp(returnOp.Req)
				// rsm.mu.Unlock()
				panic("Submit: impossible")
				// log.Printf("S%d, submit return D, origin: %v, now: %v", rsm.me, op, returnOp)
				return rpc.ErrWrongLeader, nil
			} else {
				// log.Printf("BBBS%d, submit, index %d, value %v", rsm.me, rsm.index-1, msg.Command)
				result := rsm.sm.DoOp(returnOp.Req)
				rsm.mu.Unlock()
				// log.Printf("S%d, submit return E", rsm.me)
				return rpc.OK, result
			}
		case <-ticker.C:
			rsm.mu.Lock()
			term, _ = rsm.rf.GetState()
			if rsm.term != term {
				delete(rsm.pending, op.Id)
				rsm.mu.Unlock()
				// log.Printf("S%d, submit return F", rsm.me)
				return rpc.ErrWrongLeader, nil
			}
			rsm.mu.Unlock()
		}
	}
}
