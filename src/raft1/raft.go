package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sort"
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

	leaderSignal sync.Cond
}

type NodeState int

var EmptyLogEntry = LogEntry{Term: -1, Index: -1, Command: nil}

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

	Entry        LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
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
			return
		} else {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else if args.Term > currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.state == LEADER {
			rf.state = FOLLOWER
			rf.leaderSignal.Broadcast()
		}
		reply.Term = args.Term
		if candidateIsUpToDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
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
	if args.Entry == EmptyLogEntry {
		Debug(dLog, "S%d received heartbeat from S%d: term=%d, prevLogIndex=%d, prevLogTerm=%d, entry=[%d,%d]",
			rf.me, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entry.Index, args.Entry.Term)
		// It is an empty entry
		// Leader is sending the heartbeat
		if args.Term < currentTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		reply.Term = args.Term
		if args.Term > currentTerm {
			if rf.state == LEADER {
				rf.state = FOLLOWER
				rf.leaderSignal.Broadcast()
			}
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		rf.state = FOLLOWER
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		if args.PrevLogIndex >= len(rf.log) {
			reply.Success = false
			reply.Term = rf.currentTerm
			Debug(dLog, "S%d rejected heartbeat: prevLogIndex %d >= log length %d", rf.me, args.PrevLogIndex, len(rf.log))
		} else if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Check if term matches at prevLogIndex
			reply.Success = false
			reply.Term = rf.currentTerm
			Debug(dLog, "S%d rejected heartbeat: term mismatch at index %d, expected %d, got %d",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
		} else if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			reply.Success = true
			reply.Term = rf.currentTerm
			Debug(dLog, "S%d accpeted heartbeat: term match at index %d, expected %d, got %d",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
			rf.FollowerUpdateCommitIndex(*args)
		} else {
			panic("unreachable 1")
		}
	} else {
		// Reply false if term is stale
		Debug(dLog, "S%d received append entries from S%d: term=%d, prevLogIndex=%d, prevLogTerm=%d, entry=[%d,%d]",
			rf.me, args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entry.Index, args.Entry.Term)
		if args.Term < currentTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			Debug(dLog, "S%d rejected append entries from S%d: stale term %d < %d", rf.me, args.LeaderID, args.Term, currentTerm)
			return
		}
		// Update term if newer
		if args.Term > currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			Debug(dTerm, "S%d updated term from %d to %d due to append entries", rf.me, currentTerm, args.Term)
		}
		rf.state = FOLLOWER
		// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
		if args.PrevLogIndex >= len(rf.log) {
			reply.Success = false
			reply.Term = rf.currentTerm
			Debug(dLog, "S%d rejected append entries: prevLogIndex %d >= log length %d", rf.me, args.PrevLogIndex, len(rf.log))
		} else if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Check if term matches at prevLogIndex
			reply.Success = false
			reply.Term = rf.currentTerm
			Debug(dLog, "S%d rejected append entries: term mismatch at index %d, expected %d, got %d",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
		} else if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			reply.Success = true
			reply.Term = rf.currentTerm
			Debug(dLog, "S%d accepted append entries: term match at index %d, expected %d, got %d",
				rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
			// If an existing entry conflicts with a new one (same index but different terms),
			// delete the existing entry and all that follow it (§5.3)
			if args.Entry.Index < len(rf.log) && rf.log[args.Entry.Index].Term != args.Entry.Term {
				// Conflict found, truncate log
				oldLen := len(rf.log)
				rf.log = rf.log[:args.Entry.Index]
				Debug(dLog2, "S%d found conflict at index %d, truncated log from %d to %d",
					rf.me, args.Entry.Index, oldLen, len(rf.log))
			}
			// Append new entry
			if args.Entry.Index == len(rf.log) {
				reply.Success = true
				rf.log = append(rf.log, args.Entry)
				Debug(dLog2, "S%d appended entry at index %d, term %d, log length now %d",
					rf.me, args.Entry.Index, args.Entry.Term, len(rf.log))
			}
			rf.FollowerUpdateCommitIndex(*args)
		} else {
			panic("unreachable 2")
		}
	}
	// Send heartbeat signal to reset election timeout
	select {
	case rf.heartbeatCh <- struct{}{}:
	default:
	}
}

func (rf *Raft) FollowerUpdateCommitIndex(args AppendEntriesArgs) {
	// Update commitIndex if leaderCommit > commitIndex
	// IF WE REPLY FALSE, WE CAN'T ENTER THIS CODE BLOCK
	// VERRRRRRRRRRRRRRRYYYYYYYYYYYY IMPORTANT
	// prevLogIndex & prevLogTerm check can guarantee the the follower's log before prevLogIndex
	// is exactly the same as the leader, then we can update the commitIndex
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntryIndex := len(rf.log) - 1
		oldCommitIndex := rf.commitIndex
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		Debug(dCommit, "S%d updated commitIndex from %d to %d (leaderCommit=%d)",
			rf.me, oldCommitIndex, rf.commitIndex, args.LeaderCommit)
		oldApplied := rf.lastApplied
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			applyMsg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- applyMsg
		}
		Debug(dCommit, "S%d (follower) applied, index from %d to %d", rf.me, oldApplied, rf.lastApplied)
	} else if args.LeaderCommit < rf.commitIndex {
		// Do nothing, because it is out-of-date RPC
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.state == LEADER)
	// Your code here (3B).
	if isLeader {
		index = len(rf.log)
		term = rf.currentTerm
		newEntry := LogEntry{term, index, command}
		rf.log = append(rf.log, newEntry)
		rf.matchIndex[rf.me] = len(rf.log) - 1
		index = len(rf.log) - 1
		Debug(dLog, "S%d (leader) appended new entry at index %d, term %d, log length now %d",
			rf.me, index, term, len(rf.log))
		Debug(dLog, "S%d (leader) starting log replication for entry at index %d to %d followers",
			rf.me, index, len(rf.peers)-1)
		rf.leaderSignal.Broadcast()
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
			go rf.CandidateSendRequestVote(rf.currentTerm, rf.me, len(rf.log), rf.log[len(rf.log)-1].Term)
		case FOLLOWER:
			Debug(dInfo, "S%d enters follower state for term %d", rf.me, rf.currentTerm)
			// Check if we received a heartbeat during this election timeout period
			select {
			case <-rf.heartbeatCh:
				// Received heartbeat, reset election timeout
				Debug(dInfo, "S%d received heartbeat, resetting election timeout", rf.me)
			default:
				// No heartbeat received, start election
				rf.state = CANDIDATE
				rf.currentTerm += 1
				rf.votedFor = rf.me
				Debug(dInfo, "S%d election timeout, starting election for term %d", rf.me, rf.currentTerm)
				go rf.CandidateSendRequestVote(rf.currentTerm, rf.me, len(rf.log), rf.log[len(rf.log)-1].Term)
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
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		state := rf.state
		switch state {
		case LEADER:
			// first of all, we need to commit, according to the matchIndex
			matchIndexCopy := make([]int, len(rf.matchIndex))
			copy(matchIndexCopy, rf.matchIndex)
			sort.Ints(matchIndexCopy)
			newCommitIndex := matchIndexCopy[len(rf.peers)/2]
			Debug(dCommit, "S%d checking commit index, current=%d, potential=%d", rf.me, rf.commitIndex, newCommitIndex)
			if newCommitIndex > rf.commitIndex && newCommitIndex < len(rf.log) && rf.log[newCommitIndex].Term == rf.currentTerm {
				oldCommitIndex := rf.commitIndex
				for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
					msg := raftapi.ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					}
					rf.applyCh <- msg
					rf.lastApplied = i
				}
				rf.commitIndex = newCommitIndex
				Debug(dCommit, "S%d (leader) commited, index from %d to %d", rf.me, oldCommitIndex, rf.commitIndex)
			}
			rf.leaderSignal.Broadcast()
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
	rf.log = append(rf.log, LogEntry{0, 0, struct{}{}})

	// volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// volatile state on Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.heartbeatCh = make(chan struct{}, 1)
	rf.applyCh = applyCh

	rf.leaderSignal = *sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.appendTicker()

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
					Debug(dLeader, "S%d became leader for term %d with %d votes", rf.me, currentTerm, count)
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log)
						if i == rf.me {
							rf.matchIndex[i] = len(rf.log) - 1
							continue
						}
						rf.matchIndex[i] = 0
						go rf.FollowerManager(i, rf.currentTerm)
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
				Debug(dVote, "S%d (candidate) stepping down due to higher term %d", rf.me, newTerm)
			}
			return
		}
	}
}

func (rf *Raft) FollowerManager(follower int, goroutineTerm int) {
	// fmt.Printf("S%d entering FollowerManager for S%d\n", rf.me, follower)
	// defer fmt.Printf("S%d exiting FollowerManager for S%d\n", rf.me, follower)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != LEADER || goroutineTerm != rf.currentTerm {
			Debug(dLeader, "S%d no longer leader, exiting FollowerManager for S%d", rf.me, follower)
			rf.mu.Unlock()
			return
		}
		for len(rf.log) <= rf.nextIndex[follower] {
			// No new entries to send, wait for new entries or heartbeat timer
			if len(rf.log) < rf.nextIndex[follower] {
				panic(fmt.Sprintf("leader: log len %d less than next index %d", len(rf.log), rf.nextIndex[follower]))
			}
			Debug(dLeader, "S%d waiting for heartbeat timer for S%d (nextIndex=%d, logLen=%d)",
				rf.me, follower, rf.nextIndex[follower], len(rf.log))
			rf.leaderSignal.Wait()
			if rf.state != LEADER || goroutineTerm != rf.currentTerm {
				Debug(dLeader, "S%d no longer leader, exiting FollowerManager for S%d", rf.me, follower)
				rf.mu.Unlock()
				return
			}
			// Re-read latest values after waking up from Wait()
			term := rf.currentTerm
			leaderId := rf.me
			prevLogIndex := rf.nextIndex[follower] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			leaderCommit := rf.commitIndex
			// woken up by ticker, send heartbeat
			Debug(dLeader, "S%d sending heartbeat to S%d (term=%d, prevLogIndex=%d, prevLogTerm=%d, leaderCommit=%d)",
				rf.me, follower, term, prevLogIndex, prevLogTerm, leaderCommit)
			entry := EmptyLogEntry
			args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entry, leaderCommit}
			reply := AppendEntriesReply{}
			if args.PrevLogIndex != rf.nextIndex[follower]-1 {
				panic(fmt.Sprintf("333S%d (leader) args.PrevLogIndex=%d, rf.nextIndex[%d]-1=%d", rf.me, args.PrevLogIndex, follower, rf.nextIndex[follower]-1))
			}
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(follower, &args, &reply)
			if ok {
				Debug(dLeader, "S%d received heartbeat reply from S%d: success=%v, term=%d",
					rf.me, follower, reply.Success, reply.Term)
				if !rf.LeaderHandleReply(follower, args, reply, true, goroutineTerm) {
					return
				}
			} else {
				Debug(dLeader, "S%d failed to send heartbeat to S%d (network error)", rf.me, follower)
			}
			rf.mu.Lock()
		}
		if len(rf.log) > rf.nextIndex[follower] {
			// woken up by start(), send entry
			term := rf.currentTerm
			leaderId := rf.me
			prevLogIndex := rf.nextIndex[follower] - 1
			prevLogTerm := rf.log[prevLogIndex].Term
			leaderCommit := rf.commitIndex
			entry := rf.log[prevLogIndex+1]
			Debug(dLeader, "S%d sending append entries to S%d (term=%d, prevLogIndex=%d, prevLogTerm=%d, leaderCommit=%d)",
				rf.me, follower, term, prevLogIndex, prevLogTerm, leaderCommit)
			args := AppendEntriesArgs{term, leaderId, prevLogIndex, prevLogTerm, entry, leaderCommit}
			reply := AppendEntriesReply{}
			if args.PrevLogIndex != rf.nextIndex[follower]-1 {
				panic(fmt.Sprintf("222S%d (leader) args.PrevLogIndex=%d, rf.nextIndex[%d]-1=%d", rf.me, args.PrevLogIndex, follower, rf.nextIndex[follower]-1))
			}
			rf.mu.Unlock()
			ok := rf.sendAppendEntries(follower, &args, &reply)
			if ok {
				Debug(dLog, "S%d received entry reply from S%d: success=%v, term=%d",
					rf.me, follower, reply.Success, reply.Term)
				if !rf.LeaderHandleReply(follower, args, reply, false, goroutineTerm) {
					return
				}
			} else {
				Debug(dLog, "S%d failed to send entry to S%d (network error)", rf.me, follower)
			}
			continue
		}
	}
}

func (rf *Raft) LeaderHandleReply(follower int, args AppendEntriesArgs, reply AppendEntriesReply, isHeartBeat bool, goroutineTerm int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER || goroutineTerm != rf.currentTerm {
		Debug(dLeader, "S%d is no longer leader, ignoring reply from S%d", rf.me, follower)
		return false
	}
	currentTerm := rf.currentTerm
	if !isHeartBeat {
		if !reply.Success {
			// 1. maybe the leader's term need to update
			if reply.Term > currentTerm {
				Debug(dTerm, "S%d (leader) discovered higher term %d from S%d, stepping down",
					rf.me, reply.Term, follower)
				rf.LeaderSwitchToFollower(reply.Term)
				return true
			} else {
				// 2. need to decrement nextIndex
				rf.nextIndex[follower] -= 1
				Debug(dLeader, "S%d (leader) decremented S%d nextIndex to %d via APE", rf.me, follower, rf.nextIndex[follower])
				return true
			}
		} else {
			// 3. successfully append entries
			// Check if this is a duplicate reply by ensuring we haven't already processed this entry
			if args.PrevLogIndex != rf.nextIndex[follower]-1 {
				panic(fmt.Sprintf("111S%d (leader) args.PrevLogIndex=%d, rf.nextIndex[%d]-1=%d", rf.me, args.PrevLogIndex, follower, rf.nextIndex[follower]-1))
			}
			rf.matchIndex[follower] = rf.nextIndex[follower]
			Debug(dLeader, "S%d (leader) updated S%d matchIndex to %d", rf.me, follower, rf.matchIndex[follower])
			if rf.nextIndex[follower] < len(rf.log) {
				rf.nextIndex[follower] += 1
				Debug(dLeader, "S%d (leader) updated S%d nextIndex to %d", rf.me, follower, rf.nextIndex[follower])
			} else {
				Debug(dLeader, "S%d (leader) S%d nextIndex %d reached log len %d", rf.me, follower, rf.nextIndex[follower], len(rf.log))
			}
			return true
		}
	} else {
		if !reply.Success {
			if reply.Term > currentTerm {
				rf.LeaderSwitchToFollower(reply.Term)
				return true
			} else {
				rf.nextIndex[follower] -= 1
				Debug(dLeader, "S%d (leader) decremented S%d nextIndex to %d via heartbeat", rf.me, follower, rf.nextIndex[follower])
				return true
			}
		} else {
			// Do nothing
			return true
		}
	}
}

func (rf *Raft) LeaderSwitchToFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
}
