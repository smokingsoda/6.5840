package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry // log

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on Leader
	nextIndex  []int
	matchIndex []int

	state NodeState // Node state

	heartbeatCh chan struct{}
	applyCh     chan raftapi.ApplyMsg

	asyncApplyCh chan raftapi.ApplyMsg

	termToLastIndexMap map[int]int

	lastIncludedIndex int
	lastIncludedTerm  int

	doneCh chan struct{}
}

// safeAsyncSend tries to send an ApplyMsg to rf.asyncApplyCh. If the Raft
// instance has been killed (Kill called and doneCh closed), it aborts early
// so that callers will not be blocked forever. The boolean return value
// indicates whether the message was actually delivered.
func (rf *Raft) safeAsyncSend(msg raftapi.ApplyMsg) bool {
	for !rf.killed() {
		select {
		case rf.asyncApplyCh <- msg:
			return true
		default:
			// asyncApplyCh is full – yield the processor briefly so that the
			// ApplyRoutine can make progress, then retry or detect kill.
			time.Sleep(1 * time.Millisecond)
		}
	}
	return false
}

type NodeState int

var EmptyLogEntry = LogEntry{Term: -1, Index: -1, Command: -1}

const (
	LEADER NodeState = iota
	CANDIDATE
	FOLLOWER
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	if rf.killed() {
		return -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
	Debug(dPersist, "S%d save persisted state", rf.me)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		Debug(dPersist, "S%d failed to decode persisted state, using defaults", rf.me)
		return
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		Debug(dPersist, "S%d decode persisted state", rf.me)
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
	}

}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.log[0].Index {
		Debug(dSnap, "S%d rejected snapshot: index %d < first log index %d",
			rf.me, index, rf.log[0].Index)
		return
	} else if index == rf.log[0].Index {
		Debug(dSnap, "S%d ignored snapshot: index %d already snapshotted",
			rf.me, index)
		return
	} else {
		oldLen := len(rf.log)
		rf.log = rf.log[index-rf.log[0].Index:]
		rf.lastIncludedIndex = index
		if index != rf.log[0].Index {
			panic("wrong!")
		}
		rf.lastIncludedTerm = rf.log[0].Term
		rf.persister.SetSnapshot(snapshot)
		rf.persist()
		rf.commitIndex = max(rf.lastIncludedIndex, rf.commitIndex)
		rf.lastApplied = max(rf.lastIncludedIndex, rf.lastApplied)
		// applyMsg := raftapi.ApplyMsg{
		// 	CommandValid:  false,
		// 	SnapshotValid: true,
		// 	Snapshot:      rf.persister.ReadSnapshot(),
		// 	SnapshotTerm:  rf.lastIncludedTerm,
		// 	SnapshotIndex: rf.lastIncludedIndex,
		// }
		// rf.asyncApplyCh <- applyMsg
		Debug(dSnap, "S%d created snapshot: index=%d, term=%d, log trimmed from %d to %d entries",
			rf.me, index, rf.lastIncludedTerm, oldLen, len(rf.log))
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// These two are for leader elections
	Term     int
	LeaderID int

	// We need to add those below in the future

	PrevLogIndex int
	PrevLogTerm  int

	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm       int
	ConflictFirstIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	lastLogIndex := len(rf.log) - 1 + rf.lastIncludedIndex
	lastLogTerm := rf.log[len(rf.log)-1].Term
	candidateTermIsBigger := args.LastLogTerm > lastLogTerm
	candidateIndexIsLonger := (args.LastLogTerm == lastLogTerm) && args.LastLogIndex >= lastLogIndex
	candidateIsUpToDate := candidateTermIsBigger || candidateIndexIsLonger

	if args.Term < currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == currentTerm {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateIsUpToDate {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.AcceptHeartbeat()
			return
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else if args.Term > currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		if rf.state == LEADER {
			Debug(dLeader, "S%d stepping down due to higher term by RequestVote %d", rf.me, args.Term)
		}
		rf.state = FOLLOWER
		reply.Term = args.Term
		if candidateIsUpToDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.persist()
			rf.AcceptHeartbeat()
		} else {
			reply.VoteGranted = false
		}
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dLog, "S%d rejected append entries from S%d: stale term %d < %d", rf.me, args.LeaderID, args.Term, currentTerm)
		return
	}
	if args.Entries[0] == EmptyLogEntry {
		Debug(dLog, "S%d received heartbeat from S%d: term=%d, prevLogIndex=%d, prevLogTerm=%d, entry=[%d,%d]",
			rf.me, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries[0].Index, args.Entries[0].Term)
		// It is an empty entry
		// Leader is sending the heartbeat
		reply.Term = args.Term
		if args.Term > currentTerm {
			if rf.state == LEADER {
				Debug(dLeader, "S%d stepping down due to higher term by heartbeat %d", rf.me, args.Term)
			}
			rf.state = FOLLOWER
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.state = FOLLOWER
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		if args.PrevLogIndex < rf.lastIncludedIndex {
			// If PrevLogIndex is less than lastIncludedIndex, we've already discarded these logs
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = -1
			reply.ConflictFirstIndex = rf.lastIncludedIndex
			Debug(dLog, "S%d rejected heartbeat: prevLogIndex %d < lastIncludedIndex %d", rf.me, args.PrevLogIndex, rf.lastIncludedIndex)
			rf.AcceptHeartbeat()
			return
		} else if args.PrevLogIndex >= len(rf.log)+rf.lastIncludedIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = -1
			reply.ConflictFirstIndex = len(rf.log) + rf.lastIncludedIndex
			Debug(dLog, "S%d rejected heartbeat: prevLogIndex %d >= log length %d", rf.me, args.PrevLogIndex, len(rf.log)+rf.lastIncludedIndex)
			rf.AcceptHeartbeat()
			return
		} else if args.PrevLogIndex < len(rf.log)+rf.lastIncludedIndex && rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
			// Check if term matches at prevLogIndex
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
			index := args.PrevLogIndex - rf.lastIncludedIndex
			for index > 0 && rf.log[index-1].Term == reply.ConflictTerm {
				index -= 1
			}
			reply.ConflictFirstIndex = index + rf.lastIncludedIndex
			Debug(dLog, "S%d rejected heartbeat: term mismatch at index %d, expected %d, got %d",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term)
			rf.AcceptHeartbeat()
			return
		} else if args.PrevLogIndex < len(rf.log)+rf.lastIncludedIndex && rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term == args.PrevLogTerm {
			reply.Success = true
			reply.Term = rf.currentTerm
			Debug(dLog, "S%d accpeted heartbeat: term match at index %d, expected %d, got %d",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term)
			newCommitIndex := min(args.LeaderCommit, args.PrevLogIndex)
			rf.FollowerUpdateCommitIndex(*args, newCommitIndex)
			rf.AcceptHeartbeat()
			return
		} else {
			panic("unreachable 1")
		}
	} else {
		// Reply false if term is stale
		Debug(dLog, "S%d received append entries from S%d: term=%d, prevLogIndex=%d, prevLogTerm=%d, entry=[%d,%d]",
			rf.me, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries[0].Index, args.Entries[0].Term)
		// Update term if newer
		if args.Term > currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
			if rf.state == LEADER {
				Debug(dLeader, "S%d stepping down due to higher term by AppendEntries %d", rf.me, args.Term)
			}
			rf.state = FOLLOWER
			Debug(dTerm, "S%d updated term from %d to %d due to append entries", rf.me, currentTerm, args.Term)
		}
		rf.state = FOLLOWER
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		if args.PrevLogIndex < rf.lastIncludedIndex {
			// If PrevLogIndex is less than lastIncludedIndex, we've already discarded these logs
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = -1
			reply.ConflictFirstIndex = rf.lastIncludedIndex
			Debug(dLog, "S%d rejected append entries: prevLogIndex %d < lastIncludedIndex %d", rf.me, args.PrevLogIndex, rf.lastIncludedIndex)
			rf.AcceptHeartbeat()
			return
		} else if args.PrevLogIndex >= len(rf.log)+rf.lastIncludedIndex {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = -1
			reply.ConflictFirstIndex = len(rf.log)
			Debug(dLog, "S%d rejected append entries: prevLogIndex %d >= log length %d", rf.me, args.PrevLogIndex, len(rf.log))
			rf.AcceptHeartbeat()
			return
		} else if args.PrevLogIndex < len(rf.log)+rf.lastIncludedIndex && rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
			// Check if term matches at prevLogIndex
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
			index := args.PrevLogIndex - rf.lastIncludedIndex
			for index > 0 && rf.log[index-1].Term == reply.ConflictTerm {
				index -= 1
			}
			reply.ConflictFirstIndex = index + rf.lastIncludedIndex
			Debug(dLog, "S%d rejected append entries: term mismatch at index %d, expected %d, got %d",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term)
			rf.AcceptHeartbeat()
			return
		} else if args.PrevLogIndex-rf.lastIncludedIndex < len(rf.log) && rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term == args.PrevLogTerm {
			reply.Success = true
			reply.Term = rf.currentTerm
			Debug(dLog, "S%d accepted append entries: term match at index %d, expected %d, got %d",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term)
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it (§5.3)
			if args.Entries[0].Index-rf.lastIncludedIndex < len(rf.log) {
				// Conflict found, truncate log
				oldLen := len(rf.log)
				rf.log = rf.log[:args.Entries[0].Index-rf.lastIncludedIndex]
				rf.persist()
				Debug(dLog2, "S%d found conflict at index %d, truncated log from %d to %d",
					rf.me, args.Entries[0].Index, oldLen, len(rf.log))
			} else if args.Entries[0].Index-rf.lastIncludedIndex >= len(rf.log) {
				if args.Entries[0].Index-rf.lastIncludedIndex > len(rf.log) {
					panic("aaa")
				}
				// oldLen := len(rf.log)
				// rf.log = rf.log[:args.Entry.Index]
				// rf.persist()
				// Debug(dLog2, "S%d found log longer after index %d, truncated log from %d to %d",
				// 	rf.me, args.Entry.Index, oldLen, len(rf.log))
				// fmt.Println(dLog2, "S%d found log longer after index %d, truncated log from %d to %d",
				// 	rf.me, args.Entry.Index, oldLen, len(rf.log))
			}
			// Append new entry
			if args.Entries[0].Index-rf.lastIncludedIndex == len(rf.log) {
				reply.Success = true
				rf.log = append(rf.log, args.Entries...)
				rf.persist()
				Debug(dLog2, "S%d appended entry at index %d, term %d, log length now %d",
					rf.me, args.Entries[0].Index, args.Entries[0].Term, len(rf.log))
				newCommitIndex := min(args.LeaderCommit, len(rf.log)-1+rf.lastIncludedIndex)
				rf.FollowerUpdateCommitIndex(*args, newCommitIndex)
			}
			rf.AcceptHeartbeat()
			return
		} else {
			panic("unreachable 2")
		}
	}
}

func (rf *Raft) AcceptHeartbeat() {
	select {
	case rf.heartbeatCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	if args.Term < currentTerm {
		return
	}
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// no need to make a snapshot, return
		Debug(dLog2, "S%d received InstallSnapshot, lastIncludedIndex: %d >= %d, rejected", rf.me, rf.lastIncludedIndex, args.LastIncludedIndex)
		rf.AcceptHeartbeat()
		return
	}
	// 1. truncate the log
	if args.LastIncludedIndex-rf.lastIncludedIndex >= len(rf.log) {
		rf.log = []LogEntry{EmptyLogEntry}
		Debug(dLog2, "S%d received InstallSnapshot, lastIncludedIndex: %d < %d, removed", rf.me, rf.lastIncludedIndex, args.LastIncludedIndex)

	} else {
		rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]
		Debug(dLog2, "S%d received InstallSnapshot, lastIncludedIndex: %d < %d, truncated", rf.me, rf.lastIncludedIndex, args.LastIncludedIndex)
	}
	rf.log[0].Index = args.LastIncludedIndex
	rf.log[0].Term = args.LastIncludedTerm
	rf.log[0].Command = nil

	// 2. save the snapshot
	rf.persister.SetSnapshot(args.Data)

	// 3. update the member vars
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// DO NOT FORGET TO UPDATE THESE!
	rf.commitIndex = max(rf.lastIncludedIndex, rf.commitIndex)
	rf.lastApplied = max(rf.lastIncludedIndex, rf.lastApplied)
	rf.persist()
	applyMsg := raftapi.ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	if !rf.safeAsyncSend(applyMsg) {
		return
	}
	rf.AcceptHeartbeat()
}

func (rf *Raft) FollowerUpdateCommitIndex(args AppendEntriesArgs, newCommitIndex int) {
	// Add debug info to show the values regardless of condition
	Debug(dCommit, "S%d FollowerUpdateCommitIndex called: leaderCommit=%d, rf.commitIndex=%d",
		rf.me, args.LeaderCommit, rf.commitIndex)

	// Update commitIndex if leaderCommit > commitIndex
	// IF WE REPLY FALSE, WE CAN'T ENTER THIS CODE BLOCK
	// VERRRRRRRRRRRRRRRYYYYYYYYYYYY IMPORTANT
	// prevLogIndex & prevLogTerm check can guarantee the the follower's log before prevLogIndex
	// is exactly the same as the leader, then we can update the commitIndex
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = min(args.LeaderCommit, newCommitIndex)
		Debug(dCommit, "S%d updated commitIndex from %d to %d (leaderCommit=%d)",
			rf.me, oldCommitIndex, rf.commitIndex, args.LeaderCommit)
		// oldApplied := rf.lastApplied
		if rf.lastApplied < rf.lastIncludedIndex {
			rf.lastApplied = rf.lastIncludedIndex
		}
		for rf.lastApplied < rf.commitIndex {
			if rf.killed() {
				return
			}
			rf.lastApplied++
			entry := rf.log[rf.lastApplied-rf.lastIncludedIndex]
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			if rf.lastApplied != entry.Index {
				panic("caonimade")
			}
			Debug(dCommit, "S%d (follower) applied, index=%d, entry=%v", rf.me, applyMsg.CommandIndex, applyMsg.Command)
			if !rf.safeAsyncSend(applyMsg) {
				return
			}
		}
	} else if args.LeaderCommit < rf.commitIndex {
		// Do nothing, because it is out-of-date RPC
		Debug(dCommit, "S%d ignoring stale leaderCommit %d < rf.commitIndex %d",
			rf.me, args.LeaderCommit, rf.commitIndex)
	} else {
		// args.LeaderCommit == rf.commitIndex
		Debug(dCommit, "S%d leaderCommit %d == rf.commitIndex %d, no update needed",
			rf.me, args.LeaderCommit, rf.commitIndex)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.state == LEADER)
	// Your code here (3B).
	if isLeader {
		index = len(rf.log) + rf.lastIncludedIndex
		term = rf.currentTerm
		newEntry := LogEntry{term, index, command}
		rf.log = append(rf.log, newEntry)
		rf.termToLastIndexMap[term] = index
		if newEntry.Index != index || newEntry.Index != rf.termToLastIndexMap[term] {
			panic("bbb")
		}
		rf.persist()
		rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.lastIncludedIndex
		index = len(rf.log) - 1 + rf.lastIncludedIndex
		Debug(dLog, "S%d (leader) appended new entry at index %d, term %d, log length now %d",
			rf.me, index, term, len(rf.log))
		Debug(dLog, "S%d (leader) new entry added, will be replicated by heartbeat mechanism",
			rf.me)
		// Optimization: Don't send immediately, let heartbeat mechanism handle all log replication
		// This significantly reduces the number of RPC calls
	} else {
		Debug(dLog, "S%d rejected Start() call: not leader (state=%d)", rf.me, rf.state)
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	log.Printf("S%d want to kill", rf.me)
	rf.doneCh <- struct{}{}
	log.Printf("S%d killed", rf.me)
	Debug(dWarn, "S%d has been killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		// Debug info when ticker wakes up
		ms := 150 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		state := rf.state
		switch state {
		case CANDIDATE:
			rf.state = CANDIDATE
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.persist()
			go rf.CandidateSendRequestVote(rf.currentTerm, rf.me, len(rf.log)-1+rf.lastIncludedIndex, rf.log[len(rf.log)-1].Term)
			rf.AcceptHeartbeat()
		case FOLLOWER:
			// Check if we received a heartbeat during this election timeout period
			select {
			case <-rf.heartbeatCh:
				// Received heartbeat, reset election timeout
				Debug(dLog, "S%d received heartbeat, resetting election timeout", rf.me)
			default:
				// No heartbeat received, start election
				rf.state = CANDIDATE
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.persist()
				Debug(dLog, "S%d election timeout, starting election for term %d", rf.me, rf.currentTerm)
				go rf.CandidateSendRequestVote(rf.currentTerm, rf.me, len(rf.log)-1+rf.lastIncludedIndex, rf.log[len(rf.log)-1].Term)
				rf.AcceptHeartbeat()
			}
		}
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		// Debug info when ticker wakes up
		ms := 30
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		state := rf.state
		switch state {
		case LEADER:
			// first of all, we need to commit, according to the matchIndex
			Debug(dLeader, "S%d (leader) heartbeat ticker knocks", rf.me)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.nextIndex[i] <= rf.lastIncludedIndex {
					// should call InstallSnapshot
					// need to send an InstallSnapshot RPC
					term := rf.currentTerm
					leaderID := rf.me
					lastIncludedIndex := rf.lastIncludedIndex
					lastIncludedTerm := rf.lastIncludedTerm
					offset := 0
					data := rf.persister.ReadSnapshot()
					done := true
					args := InstallSnapshotArgs{term, leaderID, lastIncludedIndex, lastIncludedTerm, offset, data, done}
					reply := InstallSnapshotReply{}
					go rf.LeaderSnapshotSendAndHandle(i, args, reply, term)
					continue

				}
				term := rf.currentTerm
				leaderId := rf.me
				prevLogIndex := rf.nextIndex[i] - 1
				prevLogTerm := rf.log[prevLogIndex-rf.lastIncludedIndex].Term
				leaderCommit := rf.commitIndex

				// Check if there are entries to send during heartbeat
				entries := []LogEntry{EmptyLogEntry}
				if rf.nextIndex[i] < len(rf.log)+rf.lastIncludedIndex {
					entries = rf.log[rf.nextIndex[i]-rf.lastIncludedIndex:]
				}
				args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
				reply := AppendEntriesReply{}
				go rf.LeaderSendAndHandle(i, args, reply, term)
			}
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.dead = 0
	rf.log = make([]LogEntry, 0) // log
	rf.log = append(rf.log, LogEntry{0, 0, 0})

	// volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// volatile state on Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.heartbeatCh = make(chan struct{}, 1)
	rf.asyncApplyCh = make(chan raftapi.ApplyMsg, 100)
	rf.applyCh = applyCh

	rf.termToLastIndexMap = make(map[int]int)

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.doneCh = make(chan struct{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.appendTicker()
	go rf.ApplyRoutine()
	return rf
}

func (rf *Raft) CandidateSendRequestVote(currentTerm int, me int, lastLogIndex int, lastLogTerm int) {
	count := 1
	args := RequestVoteArgs{currentTerm, me, lastLogIndex, lastLogTerm}
	voteCh := make(chan bool, len(rf.peers))
	termCh := make(chan int, len(rf.peers))
	Debug(dVote, "S%d (candidate) sending RequestVote to %d peers for term %d", rf.me, len(rf.peers)-1, currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				if reply.VoteGranted && reply.Term == currentTerm {
					Debug(dVote, "S%d received vote from S%d for term %d", rf.me, server, currentTerm)
					voteCh <- true
				} else if reply.Term > currentTerm {
					Debug(dVote, "S%d discovered higher term %d from S%d during election", rf.me, reply.Term, server)
					termCh <- reply.Term
				} else {
					Debug(dVote, "S%d vote rejected by S%d for term %d", rf.me, server, currentTerm)
					voteCh <- false
				}
			} else {
				Debug(dVote, "S%d failed to get vote from S%d (network error)", rf.me, server)
				voteCh <- false
			}
		}(i)
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case vote := <-voteCh:
			if vote {
				count++
			}
			if count > len(rf.peers)/2 {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if currentTerm == rf.currentTerm {
					rf.state = LEADER
					rf.termToLastIndexMap = make(map[int]int)
					for i := range rf.log {
						rf.termToLastIndexMap[rf.log[i].Term] = rf.log[i].Index
					}
					Debug(dLeader, "S%d became leader for term %d with %d votes", rf.me, currentTerm, count)
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
						if i == rf.me {
							rf.matchIndex[i] = len(rf.log) - 1 + rf.lastIncludedIndex
							continue
						}
						rf.matchIndex[i] = 0
						term := rf.currentTerm
						leaderId := rf.me
						prevLogIndex := rf.nextIndex[i] - 1
						prevLogTerm := rf.log[prevLogIndex-rf.lastIncludedIndex].Term
						leaderCommit := rf.commitIndex
						args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, []LogEntry{EmptyLogEntry}, leaderCommit}
						reply := AppendEntriesReply{}
						go rf.LeaderSendAndHandle(i, args, reply, term)
					}
				}
				return
			}
		case newTerm := <-termCh:
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if currentTerm == rf.currentTerm {
				rf.currentTerm = newTerm
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.persist()
				Debug(dVote, "S%d (candidate) stepping down due to higher term %d", rf.me, newTerm)
			}
			return
		}
	}
}

func (rf *Raft) LeaderSendAndHandle(follower int, args AppendEntriesArgs, reply AppendEntriesReply, goroutineTerm int) {
	if args.Entries[0] != EmptyLogEntry {
		Debug(dLog, "S%d sending entry to S%d (prevLogIndex=%d)", rf.me, follower, args.PrevLogIndex)
	} else {
		Debug(dLog, "S%d sending heartbeat to S%d", rf.me, follower)
	}
	ok := rf.sendAppendEntries(follower, &args, &reply)
	if ok {
		Debug(dLog, "S%d received entry reply from S%d: success=%v, term=%d",
			rf.me, follower, reply.Success, reply.Term)
		rf.LeaderHandleReply(follower, args, reply, goroutineTerm)
	} else {
		Debug(dLog, "S%d failed to send entry to S%d (network error)", rf.me, follower)
		// Optimization: Don't send immediately, let heartbeat mechanism handle all log replication
		// This significantly reduces the number of RPC calls
	}
}

func (rf *Raft) LeaderHandleReply(follower int, args AppendEntriesArgs, reply AppendEntriesReply, goroutineTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	// Check the term
	if reply.Term > currentTerm {
		// 1. maybe the leader's term need to update
		if reply.Success {
			panic("leader: higher term with successful reply")
		}
		Debug(dTerm, "S%d (leader) discovered higher term %d from S%d, stepping down",
			rf.me, reply.Term, follower)
		rf.LeaderSwitchToFollower(reply.Term)
		return
	}
	// Handle For Snapshot
	if rf.killed() || rf.state != LEADER || goroutineTerm != rf.currentTerm || args.PrevLogIndex != rf.nextIndex[follower]-1 || args.PrevLogIndex-rf.lastIncludedIndex < 0 || args.PrevLogTerm != rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term {
		// 1. Not leader anymore
		// 2. goroutine out of date
		// 3. nextIndex has been updated
		// 4. prevLogIndex now in snapshot
		// 5. Leader has been changed, so the term of the same index in the log has been modified
		Debug(dLeader, "S%d (leader) received invalid reply from S%d", rf.me, follower)
		return
	}
	if !reply.Success {
		// 2. need to decrement nextIndex
		if reply.ConflictTerm == -1 {
			rf.nextIndex[follower] = reply.ConflictFirstIndex
			Debug(dLeader, "S%d (leader) decremented S%d nextIndex to %d", rf.me, follower, rf.nextIndex[follower])
		} else {
			lastIdx, ok := rf.termToLastIndexMap[reply.ConflictTerm]
			if ok {
				rf.nextIndex[follower] = lastIdx + 1
				Debug(dLeader, "S%d (leader) decremented S%d nextIndex to %d", rf.me, follower, rf.nextIndex[follower])
			} else {
				rf.nextIndex[follower] = reply.ConflictFirstIndex
				Debug(dLeader, "S%d (leader) decremented S%d nextIndex to %d ", rf.me, follower, rf.nextIndex[follower])
			}
		}
		// Optimization: Don't send immediately, let heartbeat mechanism handle all log replication
		// This significantly reduces the number of RPC calls
	} else {
		// 3. successfully append entries
		// Check if this is a duplicate reply by ensuring we haven't already processed this entry
		if args.PrevLogIndex != rf.nextIndex[follower]-1 {
			panic(fmt.Sprintf("S%d (leader) args.PrevLogIndex=%d, rf.nextIndex[%d]-1=%d", rf.me, args.PrevLogIndex, follower, rf.nextIndex[follower]-1))
		}
		if args.Entries[0] == EmptyLogEntry {
			rf.matchIndex[follower] = args.PrevLogIndex
			rf.nextIndex[follower] = rf.matchIndex[follower] + 1
			if rf.nextIndex[follower]-rf.lastIncludedIndex > len(rf.log) {
				panic("cuowu")
			}
		} else if args.Entries[0] != EmptyLogEntry && rf.nextIndex[follower] < len(rf.log)+rf.lastIncludedIndex {
			rf.matchIndex[follower] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[follower] = rf.matchIndex[follower] + 1
			Debug(dLeader, "S%d (leader) updated S%d nextIndex to %d", rf.me, follower, rf.nextIndex[follower])
			Debug(dLeader, "S%d (leader) updated S%d matchIndex to %d", rf.me, follower, rf.matchIndex[follower])
		} else {
			if args.Entries[0] != EmptyLogEntry {
			} else {
				Debug(dLeader, "S%d (leader) S%d nextIndex %d reached log len %d", rf.me, follower, rf.nextIndex[follower], len(rf.log)+rf.lastIncludedIndex)
			}
		}
		rf.LeaderCommit()
		// Optimization: Don't send immediately, let heartbeat mechanism handle all log replication
		// This significantly reduces the number of RPC calls
		return
	}
}

func (rf *Raft) LeaderSnapshotSendAndHandle(follower int, args InstallSnapshotArgs, reply InstallSnapshotReply, goroutineTerm int) {
	Debug(dSnap, "S%d sending snapshot to S%d: lastIndex=%d, lastTerm=%d",
		rf.me, follower, args.LastIncludedIndex, args.LastIncludedTerm)
	ok := rf.sendInstallSnapshot(follower, &args, &reply)
	if ok {
		Debug(dSnap, "S%d received snapshot reply from S%d: term=%d",
			rf.me, follower, reply.Term)
		rf.LeaderHandleSnapshotRPC(follower, args, reply, goroutineTerm)
	} else {
		Debug(dSnap, "S%d failed to send snapshot to S%d (network error)",
			rf.me, follower)
	}
}

func (rf *Raft) LeaderHandleSnapshotRPC(follower int, args InstallSnapshotArgs, reply InstallSnapshotReply, goroutineTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < reply.Term {
		Debug(dSnap, "S%d discovered higher term %d from S%d during snapshot installation, stepping down",
			rf.me, reply.Term, follower)
		rf.LeaderSwitchToFollower(rf.currentTerm)
		rf.persist()
		return
	}
	if rf.state != LEADER || args.LastIncludedIndex != rf.lastIncludedIndex ||
		args.LastIncludedTerm != rf.lastIncludedTerm || args.Term != rf.currentTerm {
		Debug(dSnap, "S%d ignoring stale snapshot response from S%d", rf.me, follower)
		return
	}
	oldNextIndex := rf.nextIndex[follower]
	rf.matchIndex[follower] = args.LastIncludedIndex
	rf.nextIndex[follower] = rf.matchIndex[follower] + 1
	Debug(dSnap, "S%d updated S%d indices after snapshot: nextIndex %d->%d, matchIndex->%d",
		rf.me, follower, oldNextIndex, rf.nextIndex[follower], rf.matchIndex[follower])
	return
}

func (rf *Raft) LeaderCommit() {
	matchIndexCopy := make([]int, len(rf.matchIndex))
	copy(matchIndexCopy, rf.matchIndex)
	sort.Ints(matchIndexCopy)
	newCommitIndex := matchIndexCopy[len(rf.peers)/2]
	Debug(dCommit, "S%d checking commit index, current=%d, potential=%d", rf.me, rf.commitIndex, newCommitIndex)
	if newCommitIndex > rf.commitIndex && newCommitIndex < len(rf.log)+rf.lastIncludedIndex && rf.log[newCommitIndex-rf.lastIncludedIndex].Term == rf.currentTerm {
		oldCommitIndex := rf.commitIndex
		Debug(dCommit, "S%d (leader) ready to commit, index from %d to %d", rf.me, oldCommitIndex, rf.commitIndex)
		if rf.commitIndex+1-rf.lastIncludedIndex == 0 {
			panic("wodiao")
		}
		for i := rf.commitIndex + 1 - rf.lastIncludedIndex; i <= newCommitIndex-rf.lastIncludedIndex; i++ {
			if rf.killed() {
				return
			}
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i].Command,
				CommandIndex: i + rf.lastIncludedIndex,
			}
			Debug(dCommit, "S%d (leader) applied, index=%d, entry=%v", rf.me, msg.CommandIndex, msg.Command)
			if !rf.safeAsyncSend(msg) {
				return
			}
			rf.lastApplied = i + rf.lastIncludedIndex
		}
		rf.commitIndex = newCommitIndex
	}
}

func (rf *Raft) LeaderSwitchToFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}
}

func (rf *Raft) ApplyRoutine() {
	for {
		select {
		case msg := <-rf.asyncApplyCh:
			rf.applyCh <- msg
			Debug(dCommit, "S%d asyn-applied, index=%d, entry=%v", rf.me, msg.CommandIndex, msg.Command)
		case <-rf.doneCh:
			// Lock-free solution, and guarantee the atomicity
			close(rf.applyCh)
			return
		}
	}
}
