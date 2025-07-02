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
	state NodeState
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int

	// Event channels for state machine
	heartbeatCh       chan struct{}
	electionTimeoutCh chan struct{}

	// Election tracking
	votesReceived int
	votesNeeded   int
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
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
	isleader = (rf.state == Leader)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == currentTerm {
		if rf.votedFor == -1 {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			if rf.state == Leader {
				// should discard
			}
			return
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else if args.Term > currentTerm {
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.state = Follower
		// Reset election timeout when receiving valid vote request
		select {
		case rf.heartbeatCh <- struct{}{}:
		default:
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
		return
	} else {
		reply.Success = true
		reply.Term = args.Term
		if args.Term > currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		rf.state = Follower
		// Reset election timeout when receiving heartbeat
		select {
		case rf.heartbeatCh <- struct{}{}:
		default:
		}
		return
	}
}

// State management functions - now simplified without goroutine spawning
func (rf *Raft) becomeFollower(newTerm int) {
	Debug(dInfo, "S%d enters follower state for term %d", rf.me, newTerm)
	rf.currentTerm = newTerm
	rf.state = Follower
	rf.votedFor = -1
	rf.votesReceived = 0
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.votesReceived = 1 // Vote for self
	rf.votesNeeded = len(rf.peers)/2 + 1
	Debug(dVote, "S%d starts election in term %d", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.votedFor = -1
	rf.votesReceived = 0
	Debug(dLeader, "S%d becomes leader in term %d", rf.me, rf.currentTerm)
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
	term, isLeader = rf.GetState()

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

// Main state machine goroutine
func (rf *Raft) runStateMachine() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}
	}
}

func (rf *Raft) runFollower() {
	// Random election timeout between 150-300ms
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

	select {
	case <-rf.heartbeatCh:
		// Received heartbeat, stay as follower
		return
	case <-time.After(timeout):
		// Election timeout, become candidate
		rf.mu.Lock()
		rf.becomeCandidate()
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) runCandidate() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	// Start election
	rf.startElection(currentTerm)

	// Election timeout
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

	select {
	case <-rf.heartbeatCh:
		// Received valid heartbeat from leader, become follower
		rf.mu.Lock()
		rf.becomeFollower(rf.currentTerm)
		rf.mu.Unlock()
		return
	case <-time.After(timeout):
		// Election timeout, start new election
		rf.mu.Lock()
		rf.becomeCandidate()
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) runLeader() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	// Send heartbeats
	rf.sendHeartbeats(currentTerm)

	// Leader heartbeat interval (shorter than election timeout)
	time.Sleep(50 * time.Millisecond)
}

func (rf *Raft) startElection(term int) {
	args := RequestVoteArgs{
		Term:        term,
		CandidateId: rf.me,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Check if we're still in the same term and state
				if rf.currentTerm != term || rf.state != Candidate {
					return
				}

				if reply.VoteGranted {
					rf.votesReceived++
					if rf.votesReceived >= rf.votesNeeded {
						rf.becomeLeader()
					}
				} else if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
			}
		}(i)
	}
}

func (rf *Raft) sendHeartbeats(term int) {
	args := AppendEntriesArgs{
		Term:     term,
		LeaderID: rf.me,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := AppendEntriesReply{}
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// Check if we're still leader in the same term
				if rf.currentTerm != term || rf.state != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.becomeFollower(reply.Term)
				}
			}
		}(i)
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
	rf.state = Follower
	rf.dead = 0
	rf.votesReceived = 0
	rf.votesNeeded = 0

	rf.heartbeatCh = make(chan struct{}, 1)
	rf.electionTimeoutCh = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start main state machine goroutine
	go rf.runStateMachine()

	return rf
}
