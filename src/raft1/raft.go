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
	appendEntriesCh chan AppendEntriesMessage
	requestVoteCh   chan RequestVoteMessage
	electionCh      chan struct{}
	heartbeatCh     chan struct{}
}

type AppendEntriesMessage struct {
	Term int
}

type RequestVoteMessage struct {
	Term        int
	CandidateId int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

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
	// Debug(dPersist, "S%d persisting state: term=%d, votedFor=%d", rf.me, rf.currentTerm, rf.votedFor)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		// Debug(dPersist, "S%d no persistent state found, bootstrapping", rf.me)
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
	// Debug(dPersist, "S%d reading persistent state", rf.me)
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
	// Debug(dSnap, "S%d snapshot requested for index %d", rf.me, index)
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
	// Debug(dVote, "S%d <- S%d RequestVote: candidate term=%d, my term=%d", rf.me, args.CandidateId, args.Term, rf.currentTerm)

	currentTerm, _ := rf.GetState()
	if args.Term < currentTerm {
		// Debug(dVote, "S%d reject vote from S%d: candidate term %d < my term %d", rf.me, args.CandidateId, args.Term, currentTerm)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term == currentTerm {
		if rf.votedFor == -1 {
			// Debug(dVote, "S%d grant vote to S%d in term %d", rf.me, args.CandidateId, args.Term)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.requestVoteCh <- RequestVoteMessage{args.Term, args.CandidateId}
			return
		} else {
			// Debug(dVote, "S%d reject vote from S%d: already voted in term %d", rf.me, args.CandidateId, args.Term)
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else if args.Term > currentTerm {
		// Debug(dVote, "S%d grant vote to S%d: updating term from %d to %d", rf.me, args.CandidateId, currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.requestVoteCh <- RequestVoteMessage{args.Term, args.CandidateId}
		return
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Handler for AppendEntries RPC
	// Debug(dLog, "S%d <- S%d AppendEntries: leader term=%d, my term=%d", rf.me, args.LeaderID, args.Term, rf.currentTerm)
	currentTerm, _ := rf.GetState()
	if args.Term < currentTerm {
		// Debug(dLog, "S%d reject AppendEntries from S%d: leader term %d < my term %d", rf.me, args.LeaderID, args.Term, currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else {
		// A legal leader calls an AppendEntries RPC
		// Debug(dLog, "S%d accept AppendEntries from S%d (heartbeat) in term %d", rf.me, args.LeaderID, args.Term)
		reply.Success = true
		reply.Term = args.Term
		rf.appendEntriesCh <- AppendEntriesMessage{args.Term}
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
	// Debug(dVote, "S%d -> S%d sending RequestVote in term %d", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		// Debug(dVote, "S%d <- S%d RequestVote reply: granted=%v, term=%d", rf.me, server, reply.VoteGranted, reply.Term)
	} else {
		// Debug(dDrop, "S%d -> S%d RequestVote RPC failed", rf.me, server)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// Debug(dLog, "S%d -> S%d sending AppendEntries (heartbeat) in term %d", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		// Debug(dLog, "S%d <- S%d AppendEntries reply: success=%v, term=%d", rf.me, server, reply.Success, reply.Term)
	} else {
		// Debug(dDrop, "S%d -> S%d AppendEntries RPC failed", rf.me, server)
	}
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

	if isLeader {
		// Debug(dClient, "S%d Start() called as leader in term %d", rf.me, term)
	} else {
		// Debug(dClient, "S%d Start() called but not leader in term %d", rf.me, term)
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
	Debug(dInfo, "S%d killed", rf.me)
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
		_, isLeader := rf.GetState()
		if isLeader {
			// Debug(dTimer, "S%d ticker: sending heartbeats as leader", rf.me)
			go LeaderSendAppendEntries(rf)
		} else {
			select {
			case <-rf.heartbeatCh:
				// Debug(dTimer, "S%d ticker: received heartbeat", rf.me)
			default:
				// Debug(dTimer, "S%d ticker: triggering election", rf.me)
				rf.electionCh <- struct{}{}
			}
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

	rf.appendEntriesCh = make(chan AppendEntriesMessage)
	rf.requestVoteCh = make(chan RequestVoteMessage)
	rf.electionCh = make(chan struct{})
	rf.heartbeatCh = make(chan struct{})

	Debug(dInfo, "S%d initialized with %d peers", me, len(peers))
	// start as a follower
	go FollowerState(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func FollowerState(rf *Raft) {
	Debug(dInfo, "S%d entering FOLLOWER state in term %d", rf.me, rf.currentTerm)
	rf.isleader = false
	for {
		select {
		case aEMessage := <-rf.appendEntriesCh:
			if aEMessage.Term > rf.currentTerm {
				// Debug(dTerm, "S%d follower: updating term from %d to %d", rf.me, rf.currentTerm, aEMessage.Term)
				rf.currentTerm = aEMessage.Term
			}
		case rVMessage := <-rf.requestVoteCh:
			// Debug(dVote, "S%d follower: voting for S%d in term %d", rf.me, rVMessage.CandidateId, rVMessage.Term)
			if rVMessage.Term > rf.currentTerm {
				// Debug(dTerm, "S%d follower: updating term from %d to %d", rf.me, rf.currentTerm, rVMessage.Term)
				rf.currentTerm = rVMessage.Term
			}
		case <-rf.electionCh:
			Debug(dInfo, "S%d follower: timeout, becoming candidate", rf.me)
			go CandidateState(rf)
			return
		}
	}
}

func CandidateState(rf *Raft) {
	rf.currentTerm += 1
	rf.isleader = false
	Debug(dInfo, "S%d entering CANDIDATE state in term %d", rf.me, rf.currentTerm)

	becomeLeaderCh := make(chan struct{})
	becomeFollowerCh := make(chan struct{})
	go CandidateSendRequestVote(rf, becomeLeaderCh, becomeFollowerCh)
	for {
		select {
		case <-becomeLeaderCh:
			Debug(dLeader, "S%d candidate: won election, becoming leader in term %d", rf.me, rf.currentTerm)
			go LeaderState(rf)
			return
		case aEMessage := <-rf.appendEntriesCh:
			if aEMessage.Term > rf.currentTerm {
				// Give up candidate state, turn into follower state
				Debug(dInfo, "S%d candidate: higher term leader found, becoming follower", rf.me)
				// Debug(dTerm, "S%d candidate: updating term from %d to %d", rf.me, rf.currentTerm, aEMessage.Term)
				rf.currentTerm = aEMessage.Term
				becomeFollowerCh <- struct{}{}
				go FollowerState(rf)
				return
			}
			// else we do nothing, because we have done that in RPC handler
		case rVMessage := <-rf.requestVoteCh:
			// Debug(dVote, "S%d candidate: voting for S%d in term %d", rf.me, rVMessage.CandidateId, rVMessage.Term)
			if rVMessage.Term > rf.currentTerm {
				// Debug(dTerm, "S%d candidate: updating term from %d to %d", rf.me, rf.currentTerm, rVMessage.Term)
				rf.currentTerm = rVMessage.Term
				becomeFollowerCh <- struct{}{}
				Debug(dInfo, "S%d candidate: higher term leader found, becoming follower", rf.me)
				go FollowerState(rf)
				return
			}
		case <-rf.electionCh:
			Debug(dInfo, "S%d candidate: restarting election", rf.me)
			go CandidateState(rf)
			return
		}
	}
}

func CandidateSendRequestVote(rf *Raft, becomeLeaderCh chan struct{}, becomeFollower chan struct{}) {
	count := 1
	args := RequestVoteArgs{rf.currentTerm, rf.me}
	rf.votedFor = rf.me
	// Debug(dVote, "S%d candidate: voting for myself in term %d", rf.me, rf.currentTerm)
	// Debug(dVote, "S%d candidate: requesting votes in term %d", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		select {
		case <-becomeFollower:
			return
		default:
			if i == rf.me {
				continue
			}
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &args, &reply)
			if ok && reply.VoteGranted {
				count += 1
				// Debug(dVote, "S%d candidate: received vote from S%d, total votes=%d", rf.me, i, count)
			}
			if count > len(rf.peers)/2 {
				// Debug(dLeader, "S%d candidate: won election with %d votes", rf.me, count)
				becomeLeaderCh <- struct{}{}
				return
			}
		}
	}
	// Debug(dVote, "S%d candidate: election failed, only got %d votes", rf.me, count)
}

func LeaderState(rf *Raft) {
	Debug(dInfo, "S%d entering LEADER state in term %d", rf.me, rf.currentTerm)
	rf.isleader = true
	for {
		select {
		case aEMessage := <-rf.appendEntriesCh:
			if aEMessage.Term == rf.currentTerm {
				panic("leader received a illegal AppendEntries RPC")
			}
			// This leader is no longer legal
			// need to turn to a follower
			// Debug(dLeader, "S%d leader: received higher term AppendEntries, stepping down", rf.me)
			// Debug(dTerm, "S%d leader: updating term from %d to %d", rf.me, rf.currentTerm, aEMessage.Term)
			rf.currentTerm = aEMessage.Term
			Debug(dTerm, "S%d leader: becoming a follower", rf.me)
			go FollowerState(rf)
			return
		case rVMessage := <-rf.requestVoteCh:
			// Debug(dLeader, "S%d leader: received vote request, stepping down", rf.me)
			if rVMessage.Term > rf.currentTerm {
				// Debug(dTerm, "S%d leader: updating term from %d to %d", rf.me, rf.currentTerm, rVMessage.Term)
				rf.currentTerm = rVMessage.Term
			}
			// This leader is no longer legal
			// need to turn to a follower
			Debug(dTerm, "S%d leader: becoming a follower", rf.me)
			go FollowerState(rf)
			return
		}
	}
}

func LeaderSendAppendEntries(rf *Raft) {
	args := AppendEntriesArgs{rf.currentTerm, rf.me}
	// Debug(dLeader, "S%d leader: sending heartbeats in term %d", rf.me, rf.currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(i, &args, &reply)
	}
}
