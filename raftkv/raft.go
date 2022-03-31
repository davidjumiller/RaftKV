package raftkv

import (
	"fmt"
	"math/rand"
	"sync"

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

const (
	FOLLOWER    = 0
	CANDIDATE   = 1
	LEADER      = 2
	// CopyEntries = 3
	HeartBeat   = 4
)

type RaftState struct {
	LastIndex int
	Term      int
	IsLeader  bool
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

type Raft struct {
	mu sync.Mutex // Lock to protect shared access to this peer's state
	peers     []*util.RPCEndPoint // RPC end points of all peers
	// persister *Persister           // Object to hold this peer's persisted state
	selfidx int   // this peer's index into peers[]
	dead    int32 // set by Kill()

	// state a Raft server must maintain.
	currentTerm int
	votedFor    int        // the candidate id which got the vote
	logs        []LogEntry // logs for command and term

	commitIndex int // largest committed log index
	lastApplied int // last log index that applied to local state machine

	nextIndex  []int // For each server, the next log index that we need to send (init as leader's last log index + 1)
	matchIndex []int // For each server, the largest log index that we already sent

	identity     int
	currLeaderIdx int // the idx of current leader, will be -1 if no leader
	peersLen     int
	hbCount      int
	applyCh      chan ApplyMsg
	doAppendCh   chan int
	applyCmdLogs map[interface{}]*CommandState
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
	return RaftState{len(rf.logs), term, isleader}
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
}

func (rf *Raft) setToFollower() {
	rf.identity = FOLLOWER
}

// apply the logs entries that has committed
func (rf *Raft) apply() {
}

// provide a random timeout value
// for raft it's normally between 150ms - 300ms
func randomTimeout(min, max int) int {
	return rand.Intn(max-min) + min
}

// init and start a raft instance
func Start(peers []*util.RPCEndPoint, selfidx int,
	persister *Persister, applyCh chan ApplyMsg) *Raft
	{}

// election, should be called in a goroutine, only called when the raft instance is in candidate state
// @params: channel for passing msg if win the election
func (rf *Raft) DoElection(wonCh chan int, wgp *sync.WaitGroup) {

}