package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	currentTerm int
	votedFor    int
	state       NodeState

	heartbeatCh chan struct{}
}

type NodeState int

const (
	LEADER NodeState = iota
	CANDIDATE
	FOLLOWER
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
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

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm  int
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

	// PrevLogIndex int
	// PrevLogTerm  int

	// Entries[]
	// LeaderCommit
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	Debug(dVote, "S%d received RequestVote from S%d for term %d (current term: %d)", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%d denied vote to S%d: stale term %d < %d", rf.me, args.CandidateId, args.Term, currentTerm)
		return
	} else if args.Term == currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			Debug(dVote, "S%d granted vote to S%d for term %d", rf.me, args.CandidateId, args.Term)
			return
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			Debug(dVote, "S%d denied vote to S%d: already voted for S%d in term %d", rf.me, args.CandidateId, rf.votedFor, args.Term)
			return
		}
	} else if args.Term > currentTerm {
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER
		Debug(dTerm, "S%d updated term to %d and granted vote to S%d", rf.me, args.Term, args.CandidateId)
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	Debug(dLeader, "S%d received AppendEntries from S%d for term %d (current term: %d)", rf.me, args.LeaderID, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		Debug(dLeader, "S%d rejected AppendEntries from S%d: stale term %d < %d", rf.me, args.LeaderID, args.Term, currentTerm)
		return
	} else {
		reply.Success = true
		reply.Term = args.Term
		if args.Term > currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			Debug(dTerm, "S%d updated term to %d from AppendEntries", rf.me, args.Term)
		}
		rf.state = FOLLOWER
		Debug(dLeader, "S%d accepted AppendEntries from S%d, resetting election timeout", rf.me, args.LeaderID)
		// Send heartbeat signal to reset election timeout
		select {
		case rf.heartbeatCh <- struct{}{}:
			Debug(dInfo, "S%d sent heartbeat signal", rf.me)
		default:
			Debug(dInfo, "S%d heartbeat channel full", rf.me)
		}
		return
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

	// Your code here (3B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		// Debug info when ticker wakes up
		Debug(dInfo, "S%d election ticker woke up", rf.me)
		ms := 50 + (rand.Int63() % 300)
		Debug(dTimer, "S%d election ticker sleeping for %dms", rf.me, ms)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		state := rf.state
		switch state {
		case CANDIDATE:
			Debug(dVote, "S%d enters candidate state for term %d", rf.me, rf.currentTerm)
			rf.state = CANDIDATE
			rf.currentTerm += 1
			rf.votedFor = rf.me
			Debug(dVote, "S%d starts election in term %d", rf.me, rf.currentTerm)
			go rf.CandidateSendRequestVote(rf.currentTerm, rf.me)
		case FOLLOWER:
			Debug(dInfo, "S%d enters follower state for term %d", rf.me, rf.currentTerm)
			select {
			case <-rf.heartbeatCh:
				Debug(dInfo, "S%d received heartbeat in term %d", rf.me, rf.currentTerm)
			default:
				Debug(dVote, "S%d election timeout, becoming candidate", rf.me)
				rf.state = CANDIDATE
				rf.currentTerm += 1
				rf.votedFor = rf.me
				Debug(dVote, "S%d starts election in term %d", rf.me, rf.currentTerm)
				go rf.CandidateSendRequestVote(rf.currentTerm, rf.me)
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
		ms := 25
		Debug(dTimer, "S%d append ticker sleeping for %dms", rf.me, ms)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		Debug(dInfo, "S%d append ticker woke up", rf.me)
		rf.mu.Lock()
		state := rf.state
		switch state {
		case LEADER:
			Debug(dLeader, "S%d is leader, sending append entries in term %d", rf.me, rf.currentTerm)
			go rf.LeaderSendAppendEntries(rf.currentTerm, rf.me)
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

	rf.heartbeatCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.appendTicker()

	return rf
}

func (rf *Raft) LeaderSendAppendEntries(currentTerm int, index int) {
	// We should not hold the lock here
	// In case the term has been modified by other goroutine or RPCs
	Debug(dLeader, "S%d starting to send AppendEntries for term %d", rf.me, currentTerm)
	args := AppendEntriesArgs{currentTerm, index}
	termCh := make(chan int, len(rf.peers))

	Debug(dLeader, "S%d sending AppendEntries to %d peers", rf.me, len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			Debug(dLeader, "S%d sending AppendEntries to S%d for term %d", rf.me, server, currentTerm)
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok {
				Debug(dLeader, "S%d received AppendEntries reply from S%d: success=%v, term=%d", rf.me, server, reply.Success, reply.Term)
				if !reply.Success {
					termCh <- reply.Term
				}
			} else {
				Debug(dLeader, "S%d failed to send AppendEntries to S%d", rf.me, server)
			}
		}(i)
	}
	select {
	case newTerm := <-termCh:
		Debug(dTerm, "S%d received higher term %d, stepping down from leader", rf.me, newTerm)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if currentTerm == rf.currentTerm {
			rf.currentTerm = newTerm
			rf.state = FOLLOWER
			rf.votedFor = -1
		}
	}
	return
}

func (rf *Raft) CandidateSendRequestVote(currentTerm int, index int) {
	Debug(dVote, "S%d starting RequestVote campaign for term %d", rf.me, currentTerm)
	count := 1
	args := RequestVoteArgs{currentTerm, index}
	voteCh := make(chan bool, len(rf.peers))
	termCh := make(chan int, len(rf.peers))

	Debug(dVote, "S%d sending RequestVote for term %d to %d peers", rf.me, currentTerm, len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			Debug(dVote, "S%d sending RequestVote to S%d for term %d", rf.me, server, currentTerm)
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				Debug(dVote, "S%d received RequestVote reply from S%d: granted=%v, term=%d", rf.me, server, reply.VoteGranted, reply.Term)
				if reply.VoteGranted && reply.Term == currentTerm {
					Debug(dVote, "S%d received vote from S%d for term %d", rf.me, server, currentTerm)
					voteCh <- true
				} else if reply.Term > currentTerm {
					Debug(dTerm, "S%d received higher term %d from S%d (current: %d)", rf.me, reply.Term, server, currentTerm)
					termCh <- reply.Term
				} else {
					Debug(dVote, "S%d vote denied by S%d for term %d (reply term: %d)", rf.me, server, currentTerm, reply.Term)
					voteCh <- false
				}
			} else {
				Debug(dVote, "S%d failed to send RequestVote to S%d", rf.me, server)
				voteCh <- false
			}
		}(i)
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case vote := <-voteCh:
			if vote {
				count++
				Debug(dVote, "S%d vote count: %d/%d (need %d)", rf.me, count, len(rf.peers), len(rf.peers)/2+1)
			}
			if count > len(rf.peers)/2 {
				Debug(dLeader, "S%d won election with %d votes in term %d", rf.me, count, currentTerm)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if currentTerm == rf.currentTerm {
					rf.state = LEADER
				}
				return
			}
		case newTerm := <-termCh:
			Debug(dTerm, "S%d stepping down to follower due to higher term %d", rf.me, newTerm)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if currentTerm == rf.currentTerm {
				rf.currentTerm = newTerm
				rf.state = FOLLOWER
				rf.votedFor = -1
			}
			return
		}
	}
	Debug(dVote, "S%d election failed with %d votes in term %d", rf.me, count, currentTerm)
}
