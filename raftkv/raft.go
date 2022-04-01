package raftkv

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"cs.ubc.ca/cpsc416/p1/util"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

type CommandState struct {
	Term  int
	Index int
}

type IdentityType int

const (
	FOLLOWER  IdentityType = 0
	CANDIDATE              = 1
	LEADER                 = 2
	// CopyEntries = 3
	HeartBeat = 4
)

type RaftState struct {
	LastIndex int
	Term      int
	IsLeader  bool
	LeaderID  int
}

type RequestVoteArgs struct {
	Term         int // Candidate term
	CandidateId  int // Candidate ID
	LastLogIndex int // Candidate's last log index
	LastLogTerm  int // Candidate's term of last log index
}

type RequestVoteReply struct {
	Term        int  // Current Term
	VoteGranted bool // True if candidate is accepted, false if candidate vote is rejected
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader's id
	PrevLogIndex int        // previous log index
	PrevLogTerm  int        // term of previous log index's log
	Entries      []LogEntry // logs that need to be persisted
	LeaderCommit int        // last index of committed log
}

type AppendEntriesReply struct {
	Term         int  // current term, used by the leader to update itself
	Success      bool // true if PrevLogIndex and PrevLogTerm is matched (for consistency)
	PrevLogIndex int
}

type HBMsg struct {
}

type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*util.RPCEndPoint // RPC end points of all peers
	// persister *Persister           // Object to hold this peer's persisted state
	selfidx int  // this peer's index into peers[]
	dead    bool // set by Kill()

	// state a Raft server must maintain.
	currentTerm int
	votedFor    int        // the candidate id which got the vote
	voteCount   int        // count of vote in this round of election
	logs        []LogEntry // logs for command and term

	commitIndex int // largest committed log index
	lastApplied int // last log index that applied to local state machine

	nextIndex  []int // For each server, the next log index that we need to send (init as leader's last log index + 1)
	matchIndex []int // For each server, the largest log index that we already sent

	identity      IdentityType
	currLeaderIdx int // the idx of current leader, will be -1 if no leader
	peersLen      int
	hbCount       int
	applyCh       chan ApplyMsg

	// doAppendCh    chan int
	applyCmdLogs map[interface{}]*CommandState

	winElectCh chan bool
	stepDownCh chan bool
	voteCh     chan bool
	hbCh       chan HBMsg
}

//
// reset the channels, needed when converting server state.
// lock must be held before calling this.
//
func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool, 1)
	rf.stepDownCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, 1)
}

// RequestVote endpoint
// called when other raft instances are candidate and this instance is follower
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
}

// AppendEntries endpoint
// called by the leader of the raft
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() RaftState {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("current term of %v: %v \n", rf.selfidx, rf.currentTerm)
	term = rf.currentTerm
	isleader = rf.identity == LEADER
	return RaftState{len(rf.logs) - 1, term, isleader, rf.currLeaderIdx}
}

// Execute a command, called when the server gets a request
func (rf *Raft) Execute(command interface{}) (RaftState, error) {

}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() {
}

//
// restore previously persisted state.
// can be left to m2/m3
//
func (rf *Raft) readPersist(data []byte) {
}

// broadcast request vote requests to all peers
// must be called after lock is held
func (rf *Raft) broadcastRequestVote() {
	args := &RequestVoteArgs{}
	args.CandidateId = rf.selfidx
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	reply := &RequestVoteReply{}

	for i := range rf.peers {
		if i == rf.selfidx {
			continue
		}

		go rf.sendRequestVote(i, args, reply)
	}
}

//
// send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
func (rf *Raft) sendRequestVote(serverIdx int, args *RequestVoteArgs, reply *RequestVoteReply) {
	err := rf.peers[serverIdx].Call("Raft.RequestVote", args, reply)
	if err != nil {
		log.Printf("error in rpc call server from %v to %v: %v \n", rf.selfidx, serverIdx, err)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.identity != CANDIDATE || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.setToFollower(args.Term)
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount >= len(rf.peers)/2+1 {
			rf.winElectCh <- true
		}
	}

}

func (rf *Raft) broadcastAppendEntries() {

}

func (rf *Raft) sendAppendEntries(serverIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	err := rf.peers[serverIdx].Call("Raft.AppendEntries", args, reply)
	return err
}

func (rf *Raft) setToFollower(term int) {
	ident := rf.identity
	rf.identity = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.voteCount = 0

	if ident != FOLLOWER {
		rf.stepDownCh <- true
	}
}

// set raft state to candidate
func (rf *Raft) setToCandidate(identityType IdentityType) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.identity != identityType {
		return
	}
	rf.resetChannels()
	rf.identity = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.selfidx
	rf.voteCount = 1

	// TODO: implment persist and broadcast
	// rf.persist()
	// rf.broadcastRequestVote()
}

func (rf *Raft) setToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.identity != CANDIDATE {
		return
	}

	rf.resetChannels()
	rf.identity = LEADER
	rf.currLeaderIdx = rf.selfidx
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// init to the index of last log + 1: len(rf.logs) -1 + 1
	lastIndex := len(rf.logs)
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
	}

	// TODO: broadcast appendentries
	// rf.broadcastAppendEntries()
}

// apply the logs entries that has committed
func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.lastApplied = i
	}
}

// provide a random timeout value
// for raft it's normally between 150ms - 300ms
func randomTimeout(min, max int) int {
	return rand.Intn(max-min) + min
}

// init and start a raft instance
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[selfidx]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Start() must return quickly, so it should start goroutines
// for any long-running work.
// hbCh is the channel for hb msg, the server should setup listener and push
// hb into this channel
//
func Start(peers []*util.RPCEndPoint, selfidx int,
	persister *util.Persister, applyCh chan ApplyMsg, hbCh chan HBMsg) *Raft {
	rf := &Raft{}
	rf.dead = false
	rf.peers = peers
	rf.peersLen = len(peers)
	rf.selfidx = selfidx
	rf.currLeaderIdx = -1
	rf.applyCh = applyCh
	rf.hbCh = hbCh

	fmt.Printf("----- %v Start -----", rf.selfidx)
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{
		Command: "Start",
		Term:    0,
		Index:   0,
	})
	rf.setToFollower(rf.currentTerm)
	rf.nextIndex = make([]int, rf.peersLen)
	rf.matchIndex = make([]int, rf.peersLen)
	rf.applyCmdLogs = make(map[interface{}]*CommandState)

	rf.resetChannels()

	rand.Seed(time.Now().UnixNano())

	// raft process called in a goroutine to keep running in the background
	go rf.runRaft()
	return rf
}

func (rf *Raft) runRaft() {
	for !rf.dead {
		rf.mu.Lock()
		switch rf.identity {
		case FOLLOWER:
			rf.mu.Unlock()
			select {
			case <-rf.hbCh:
			case <-time.After(time.Duration(randomTimeout(700, 1000)) * time.Millisecond):
				rf.setToCandidate(FOLLOWER)
			}
		case CANDIDATE:
			rf.mu.Unlock()
			// TODO: candidate round
			select {
			case <-rf.stepDownCh:
			// set to follower will push to stepdown channel
			// if it's this case then it's already a follower
			case <-rf.winElectCh:
				rf.setToLeader()
			case <-time.After(time.Duration(randomTimeout(700, 1000)) * time.Millisecond):
				rf.setToCandidate(CANDIDATE)
			}

		case LEADER:
			rf.mu.Unlock()
			select {
			case <-rf.stepDownCh:
			// same as above
			case <-time.After(120 * time.Millisecond):
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		}

	}
}
