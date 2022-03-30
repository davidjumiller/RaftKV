package raftkv

import (
	"fmt"
	"sync"
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
	CopyEntries = 3
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
	// peers     []*rpcutil.ClientEnd // RPC end points of all peers
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
	peersLen     int
	hbCount      int
	applyCh      chan ApplyMsg
	doAppendCh   chan int
	applyCmdLogs map[interface{}]*CommandState
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
}

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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() {
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
}
