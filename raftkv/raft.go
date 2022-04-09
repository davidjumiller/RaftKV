package raftkv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"net/rpc"
	"sync"
	"time"

	"cs.ubc.ca/cpsc416/p1/util"
	"github.com/DistributedClocks/tracing"
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
	Token        tracing.TracingToken
}

type RequestVoteReply struct {
	Term        int  // Current Term
	VoteGranted bool // True if candidate is accepted, false if candidate vote is rejected
	Token       tracing.TracingToken
}

type AppendEntriesArgs struct {
	Term         int                  // leader's term
	LeaderId     int                  // leader's id
	PrevLogIndex int                  // previous log index
	PrevLogTerm  int                  // term of previous log index's log
	Entries      []LogEntry           // logs that need to be persisted
	LeaderCommit int                  // last index of committed log
	Token        tracing.TracingToken // token for tracing
}

type AppendEntriesReply struct {
	Term          int  // current term, used by the leader to update itself
	Success       bool // true if PrevLogIndex and PrevLogTerm is matched (for consistency)
	PrevLogIndex  int
	ConflictTerm  int // -1 if no conflict, otherwise it's the smallest term number where leader doesn't agree with the follower
	ConflictIndex int // -1 if no conflict, otherwise it's the smallest log index of conflict
	Token         tracing.TracingToken
}

type HBMsg struct {
	Term     int
	LeaderId int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*util.RPCEndPoint // RPC end points of all peers
	persister *util.Persister     // Object to hold this peer's persisted state
	selfidx   int                 // this peer's index into peers[]
	dead      bool                // set by Kill()

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
	applyCh       chan ApplyMsg

	winElectCh chan bool
	stepDownCh chan bool
	voteCh     chan bool
	hbCh       chan HBMsg

	// for tracing purpose
	rtrace *tracing.Trace // record the raft's lifetime event (e.g. start, end, request, response)
}

// struct for tracing
type RaftStart struct {
	Idx int
}

type RaftEnd struct {
	Idx int
}

type ReceiveRequestVote struct {
	Term         int // Candidate term
	CandidateId  int // Candidate ID
	LastLogIndex int // Candidate's last log index
	LastLogTerm  int // Candidate's term of last log index
	ReceiveID    int // ID of raft instace that receives the request vote
}

type SendRequestVote struct {
	Term         int // Candidate term
	CandidateId  int // Candidate ID
	LastLogIndex int // Candidate's last log index
	LastLogTerm  int // Candidate's term of last log index
	SendID       int // ID of raft instance that sends the request vote
}

type RequestVoteRes struct {
	Term        int  // Current Term
	VoteGranted bool // True if candidate is accepted, false if candidate vote is rejected
	ReceiveID   int  // ID of receiver
	SendID      int  // ID of sender
}

type LeaderElected struct {
	Term     int
	LeaderID int
}

type SendAppendEntries struct {
	Term         int // leader's term
	LeaderId     int // leader's id
	PrevLogIndex int // previous log index
	PrevLogTerm  int // term of previous log index's log
	LeaderCommit int // last index of committed log
	SendID       int // ID of raft instance that sends the request vote
}

type ReceiveAppendEntries struct {
	Term         int // leader's term
	LeaderId     int // leader's id
	PrevLogIndex int // previous log index
	PrevLogTerm  int // term of previous log index's log
	LeaderCommit int // last index of committed log
	ReceiveID    int // ID of raft instace that receives the request vote
}

type AppendEntriesRes struct {
	Term          int  // current term, used by the leader to update itself
	Success       bool // true if PrevLogIndex and PrevLogTerm is matched (for consistency)
	PrevLogIndex  int
	ConflictTerm  int // -1 if no conflict, otherwise it's the smallest term number where leader doesn't agree with the follower
	ConflictIndex int // -1 if no conflict, otherwise it's the smallest log index of conflict
}

type ExecuteCommand struct {
	Term     int
	LeaderID int
	Command  interface{}
}

type Commit struct {
	ID    int
	Term  int
	Index int
}

type Apply struct {
	ID           int
	Term         int
	CommandValid bool
	Command      interface{}
	CommandIndex int
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	oldTerm := rf.currentTerm // TODO: Give this a more descriptive name
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	trace := rf.rtrace.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ReceiveRequestVote{args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.selfidx})

	reply.Token = trace.GenerateToken()
	if args.Term < rf.currentTerm {
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.setToFollower(args.Term)
	}

	if (rf.votedFor < 0 || rf.votedFor == args.CandidateId) && rf.checkLogConsistency(args.LastLogIndex, args.LastLogTerm, oldTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.voteCh <- true
	}

	return nil

}

// check if the log in leader is longer than the log in the follower
func (rf *Raft) checkLogConsistency(cLastIdx, cLastTerm, term int) bool {
	if cLastTerm == term {
		return cLastIdx >= len(rf.logs)-1
	}

	return cLastTerm > term
}

// AppendEntries endpoint
// called by the leader of the raft
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// init reply
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	trace := rf.rtrace.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ReceiveAppendEntries{args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.selfidx})

	reply.Token = trace.GenerateToken()
	if args.Term < rf.currentTerm {
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.setToFollower(args.Term)
	}

	lastIndex := len(rf.logs) - 1
	rf.hbCh <- HBMsg{args.Term, args.LeaderId}

	// follower's log is shorter than the leader
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return nil
	}

	// check if the term matches
	if cfTerm := rf.logs[args.PrevLogIndex].Term; cfTerm != args.PrevLogTerm {
		reply.ConflictTerm = cfTerm
		for i := args.PrevLogIndex; i >= 0 && rf.logs[i].Term == cfTerm; i-- {
			reply.ConflictIndex = i
		}
		reply.Success = false
		return nil
	}

	// check the args.Entries to see if it matches with rf.logs
	// break when it finds the first
	i, j := args.PrevLogIndex+1, 0
	for ; i <= lastIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.logs[i].Term != args.Entries[j].Term {
			break
		}
	}

	// truncate the log, and use args.Entries to fill in the log
	// to keep log consistency
	rf.logs = rf.logs[:i]
	args.Entries = args.Entries[j:]
	rf.logs = append(rf.logs, args.Entries...)

	reply.Success = true

	// update commit index to min(leaderCommit, lastIndex)
	if args.LeaderCommit > rf.commitIndex {
		lastIndex = len(rf.logs) - 1
		if args.LeaderCommit < lastIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastIndex
		}

		go rf.apply()
	}

	rf.currLeaderIdx = args.LeaderId

	return nil

}

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
func (rf *Raft) Execute(command interface{}) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.rtrace.RecordAction(ExecuteCommand{rf.currentTerm, rf.currLeaderIdx, command})
	rf.logs = append(rf.logs, LogEntry{command, rf.currentTerm, len(rf.logs)})

	return nil
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.logs) != nil {
		fmt.Println("Error in persist encoding")
		return
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.persister.Persist()
}

//
// restore previously persisted state.
// can be left to m2/m3
//
func (rf *Raft) readPersist() {
	rf.persister.ReadPersist()
	data := rf.persister.GetRaftState()

	// check whether the data is empty
	if data == nil || len(data) < 1 {
		return
	}

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	if dec.Decode(&rf.currentTerm) != nil ||
		dec.Decode(&rf.votedFor) != nil ||
		dec.Decode(&rf.logs) != nil {
		fmt.Println("error decoding log file data")
	}
}

// broadcast request vote requests to all peers
// must be called after lock is held
func (rf *Raft) broadcastRequestVote() {
	for i := range rf.peers {
		if i == rf.selfidx {
			continue
		}
		args := &RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = rf.selfidx
		args.LastLogIndex = len(rf.logs) - 1
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		reply := &RequestVoteReply{}

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
	trace := rf.rtrace.Tracer.CreateTrace()
	trace.RecordAction(SendRequestVote{args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.selfidx})
	args.Token = trace.GenerateToken()
	err := rf.peers[serverIdx].Call("Raft.RequestVote", args, reply)
	if err != nil {
		fmt.Printf("error in rpc call server from %v to %v: %v \n", rf.selfidx, serverIdx, err)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	trace = rf.rtrace.Tracer.ReceiveToken(reply.Token)
	trace.RecordAction(RequestVoteRes{reply.Term, reply.VoteGranted, serverIdx, rf.selfidx})

	if rf.identity != CANDIDATE || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.setToFollower(reply.Term)
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount >= len(rf.peers)/2+1 {
			rf.winElectCh <- true
			trace.RecordAction(LeaderElected{rf.currentTerm, rf.selfidx})
		}
	}

}

// broadcast appendEntries requests to all peers
// must be called after lock is held
func (rf *Raft) broadcastAppendEntries() {
	if rf.identity != LEADER {
		return
	}
	args := &AppendEntriesArgs{}
	args.LeaderId = rf.selfidx
	args.LeaderCommit = rf.commitIndex
	args.Term = rf.currentTerm

	reply := &AppendEntriesReply{}

	for i := range rf.peers {
		if i == rf.selfidx {
			continue
		}

		args.PrevLogIndex = rf.nextIndex[i] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term

		// send all entries after rf.nextIndex[i]
		entries := rf.logs[rf.nextIndex[i]:]
		args.Entries = make([]LogEntry, len(entries))
		copy(args.Entries, entries)

		go rf.sendAppendEntries(i, args, reply)
	}
}

func (rf *Raft) sendAppendEntries(serverIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	trace := rf.rtrace.Tracer.CreateTrace()
	trace.RecordAction(SendAppendEntries{args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.selfidx})
	args.Token = trace.GenerateToken()
	err := rf.peers[serverIdx].Call("Raft.AppendEntries", args, reply)
	if err != nil {
		fmt.Printf("error in rpc call server from %v to %v: %v \n", rf.selfidx, serverIdx, err)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	trace = trace.Tracer.ReceiveToken(reply.Token)
	trace.RecordAction(AppendEntriesRes{reply.Term, reply.Success, reply.PrevLogIndex, reply.ConflictTerm, reply.ConflictIndex})
	if rf.identity != LEADER || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.setToFollower(args.Term)
		return
	}

	// update matchIndex and nextIndex of followers
	if reply.Success {
		newMatchIdx := args.PrevLogIndex + len(args.Entries)
		if newMatchIdx > rf.matchIndex[serverIdx] {
			rf.matchIndex[serverIdx] = newMatchIdx
		}

		rf.nextIndex[serverIdx] = newMatchIdx + 1
	} else if reply.ConflictTerm < 0 {
		// follower's log shorter than leader's log
		rf.nextIndex[serverIdx] = reply.ConflictIndex
		rf.matchIndex[serverIdx] = reply.ConflictIndex - 1
	} else {
		// find the conflict term in log
		newNextIndex := len(rf.logs) - 1
		for ; newNextIndex >= 0; newNextIndex-- {
			if rf.logs[newNextIndex].Term == reply.ConflictTerm {
				break
			}
		}

		// if not found, set next index to conflict index
		if newNextIndex < 0 {
			rf.nextIndex[serverIdx] = reply.ConflictIndex
		} else {
			rf.nextIndex[serverIdx] = newNextIndex
		}

		rf.matchIndex[serverIdx] = newNextIndex - 1
	}

	rf.Commit(trace)
}

// if there's an idx i where i >= rf.commitIndex and
// for majority peers, the matchIdx of that peer >= i (it's sent to more than majority of peers)
// update commitIdx to i and apply
func (rf *Raft) Commit(trace *tracing.Trace) {
	for i := len(rf.logs) - 1; i >= rf.commitIndex; i-- {
		sentCount := 1 // count itself

		for j := range rf.peers {
			if j == rf.selfidx {
				continue
			}
			if rf.matchIndex[j] >= i {
				sentCount++
			}
		}

		if sentCount >= len(rf.peers)+1 {
			rf.commitIndex = i
			trace.RecordAction(Commit{rf.selfidx, rf.currentTerm, i})
			go rf.apply()
			break // find the latest idx that hasn't committed but already sent to majority
		}
	}
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

	// avoid data racing
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
	rf.broadcastRequestVote()
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

	rf.broadcastAppendEntries()
}

// apply the logs entries that has committed
func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- applyMsg
		rf.rtrace.RecordAction(Apply{rf.selfidx, rf.currentTerm, applyMsg.CommandValid, applyMsg.Command, applyMsg.CommandIndex})
		rf.lastApplied = i
	}
}

// provide a random timeout value
// for raft it's normally between 150ms - 300ms
func randomTimeout(min, max int) int {
	return rand.Intn(max-min) + min
}

func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.dead = true
	rf.rtrace.RecordAction(RaftEnd{rf.selfidx})
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
func StartRaft(peers []*util.RPCEndPoint, selfidx int,
	persister *util.Persister, applyCh chan ApplyMsg, tracer *tracing.Tracer) (*Raft, error) {
	rf := &Raft{}
	rf.dead = false
	rf.peers = peers
	rf.peersLen = len(peers)
	rf.selfidx = selfidx
	rf.currLeaderIdx = 0 // M2: We will assume 0 is the leader
	rf.applyCh = applyCh

	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{
		Command: "Start",
		Term:    0,
		Index:   0,
	})
	rf.setToFollower(rf.currentTerm)
	rf.nextIndex = make([]int, rf.peersLen)
	rf.matchIndex = make([]int, rf.peersLen)
	rf.hbCh = make(chan HBMsg, rf.peersLen)

	rf.resetChannels()

	rand.Seed(time.Now().UnixNano())

	rf.rtrace = tracer.CreateTrace()

	rf.rtrace.RecordAction(RaftStart{rf.selfidx})

	_, err := util.StartRPCListener(rf.peers[selfidx].Addr)
	if err != nil {
		fmt.Printf("listener error: %v \n", err)
		return nil, err
	}

	rpc.Register(rf)
	// raft process called in a goroutine to keep running in the background
	go rf.runRaft()
	return rf, nil
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
