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
	isleader bool
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int

	// Inner chans for state switch
	toFollower  chan struct{}
	heartbeatCh chan struct{}
	tickerCh    chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	isleader = rf.isleader
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
	// Your code here (3A, 3B).
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	} else if args.Term == currentTerm {
		if rf.votedFor == -1 {
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			if rf.isleader {
				panic("leader received a same term R_RPC")
			}
			rf.mu.Unlock()
			return
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.mu.Unlock()
			return
		}
	} else if args.Term > currentTerm {
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.isleader = false
		// Need to unlock in advance
		// Becasue it holds both toFollower chan and the lock
		rf.mu.Unlock()
		rf.toFollower <- struct{}{}
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	} else {
		reply.Success = true
		reply.Term = args.Term
		if args.Term > currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		rf.isleader = false
		// Need to unlock in advance
		// Because it holds both toFollower chan and lock
		rf.mu.Unlock()
		rf.toFollower <- struct{}{}
		select {
		case rf.heartbeatCh <- struct{}{}:
		default:
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		select {
		case <-rf.heartbeatCh:
		default:
			rf.tickerCh <- struct{}{}
		}
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
	rf.isleader = false
	rf.dead = 0

	rf.toFollower = make(chan struct{})
	rf.heartbeatCh = make(chan struct{}, 1)
	rf.tickerCh = make(chan struct{})

	// start as a follower
	go FollowerState(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func FollowerState(rf *Raft) {
	rf.mu.Lock()
	rf.isleader = false
	rf.mu.Unlock()
	for {
		select {
		case <-rf.toFollower:
			continue
		case <-rf.tickerCh:
			go CandidateState(rf)
			return
		}
	}
}

func CandidateState(rf *Raft) {
	rf.mu.Lock()
	rf.currentTerm += 1
	currentTerm := rf.currentTerm
	rf.votedFor = rf.me
	rf.mu.Unlock()
	toMain_BecomeLeader := make(chan struct{})
	toMain_BecomeFollower := make(chan int)
	toSub_StopSending := make(chan struct{})
	go CandidateSendRequestVote(rf, toMain_BecomeLeader, toMain_BecomeFollower, toSub_StopSending, currentTerm, rf.me)
	for {
		select {
		case <-toMain_BecomeLeader:
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.isleader = true
			go LeaderState(rf)
			return
		case <-rf.toFollower:
			go FollowerState(rf)
			return
		case newTerm := <-toMain_BecomeFollower:
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.currentTerm = newTerm
			go FollowerState(rf)
			return
		case <-rf.tickerCh:
			go CandidateState(rf)
			return
		}
	}
}

func CandidateSendRequestVote(rf *Raft, toMain_BecomeLeader chan struct{}, toMain_BecomeFollower chan int, toSub_StopSending chan struct{}, currentTerm int, index int) {
	count := 1
	args := RequestVoteArgs{currentTerm, index}
	voteCh := make(chan bool, len(rf.peers))
	termCh := make(chan int, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				if reply.VoteGranted && reply.Term == currentTerm {
					voteCh <- true
				} else if reply.Term > currentTerm {
					termCh <- reply.Term
				} else {
					voteCh <- false
				}
			} else {
				voteCh <- false
			}
		}(i)
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case <-toSub_StopSending:
			return
		case vote := <-voteCh:
			if vote {
				count++
			}
			if count > len(rf.peers)/2 {
				toMain_BecomeLeader <- struct{}{}
				return
			}
		case newTerm := <-termCh:
			toMain_BecomeFollower <- newTerm
			return
		}
	}
}

func LeaderState(rf *Raft) {
	rf.mu.Lock()
	rf.isleader = true
	rf.mu.Unlock()
	toMain_BecomeFollower := make(chan int)
	toSub_StopSending := make(chan struct{})
	go LeaderSendAppendEntries(rf, toMain_BecomeFollower, toSub_StopSending)
	for {
		select {
		case <-rf.toFollower:
			toSub_StopSending <- struct{}{}
			go FollowerState(rf)
			return
		case <-rf.tickerCh:
			toSub_StopSending <- struct{}{}
			go LeaderSendAppendEntries(rf, toMain_BecomeFollower, toSub_StopSending)
		case newTerm := <-toMain_BecomeFollower:
			rf.mu.Lock()
			rf.currentTerm = newTerm
			rf.mu.Unlock()
			go FollowerState(rf)
			return
		}
	}
}

func LeaderSendAppendEntries(rf *Raft, toMain_BecomeFollower chan int, toSub_StopSending chan struct{}) {
	rf.mu.Lock()
	args := AppendEntriesArgs{rf.currentTerm, rf.me}
	rf.mu.Unlock()

	termCh := make(chan int, len(rf.peers))

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, &args, &reply)
			if ok && !reply.Success {
				termCh <- reply.Term
			}
		}(i)
	}

	select {
	case <-toSub_StopSending:
		return
	case newTerm := <-termCh:
		toMain_BecomeFollower <- newTerm
		return
	}
}
