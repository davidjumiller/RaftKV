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

/*
	Thanks to [MIT 6.824 Student Repo](https://github.com/WenbinZhu/mit-6.824-labs)
	The repo shows a implementation of raft for us to reason about
	how exactly we should implement it in Golang with RPC
*/

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
	Entries      []LogEntry           // Logs that need to be persisted
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
	Mutex     sync.Mutex          // Lock to protect shared access to this peer's state
	Peers     []*util.RPCEndPoint // RPC end points of all Peers
	Persister *util.Persister     // Object to hold this peer's persisted state
	SelfIndex int                 // this peer's index into Peers[]
	Dead      bool                // set by Kill()

	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor    int        // the candidate id which got the vote
	VoteCount   int        // count of vote in this round of election
	Logs        []LogEntry // Logs for command and term

	CommitIndex int // largest committed log index
	LastApplied int // last log index that applied to local state machine

	NextIndex  []int // For each server, the next log index that we need to send (init as leader's last log index + 1)
	MatchIndex []int // For each server, the largest log index that we already sent

	Identity        IdentityType
	CurrLeaderIndex int // the idx of current leader, will be -1 if no leader
	PeersLen        int
	ApplyCh         chan ApplyMsg

	WinElectCh chan bool
	StepDownCh chan bool
	VoteCh     chan bool
	HbCh       chan HBMsg

	// for tracing purpose
	RTrace *tracing.Trace // record the raft's lifetime event (e.g. start, end, request, response)
}

type RemoteRaft struct {
	Raft *Raft
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
	Entries      []LogEntry
}

type ReceiveAppendEntries struct {
	Term         int // leader's term
	LeaderId     int // leader's id
	PrevLogIndex int // previous log index
	PrevLogTerm  int // term of previous log index's log
	LeaderCommit int // last index of committed log
	ReceiveID    int // ID of raft instace that receives the request vote
	Entries      []LogEntry
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
	Command  util.RaftPutReq
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

type ReadPersist struct {
	CurrentTerm int
	VotedFor    int
	// LogsLength  int
	Logs []LogEntry
}

// struct for persisting encode/decode
type PersistData struct {
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry
}

//
// reset the channels, needed when converting server state.
// lock must be held before calling this.
//
func (rf *Raft) resetChannels() {
	rf.WinElectCh = make(chan bool, 1)
	rf.StepDownCh = make(chan bool, 1)
	rf.VoteCh = make(chan bool, 1)
}

// RequestVote endpoint
// called when other raft instances are candidate and this instance is follower
func (remoteRaft *RemoteRaft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf := remoteRaft.Raft
	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()
	defer rf.persist()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	trace := rf.RTrace.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ReceiveRequestVote{args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.SelfIndex})

	reply.Token = trace.GenerateToken()
	if args.Term < rf.CurrentTerm {
		return nil
	}

	if args.Term > rf.CurrentTerm {
		rf.setToFollower(args.Term)
	}

	if (rf.VotedFor < 0 || rf.VotedFor == args.CandidateId) && rf.checkLogConsistency(args.LastLogIndex, args.LastLogTerm) {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true

		rf.VotedFor = args.CandidateId
		rf.VoteCh <- true
	}

	return nil

}

// check if the log in leader is longer than the log in the follower
func (rf *Raft) checkLogConsistency(cLastIdx, cLastTerm int) bool {
	lastIdx := len(rf.Logs) - 1
	lastLog := rf.Logs[lastIdx]
	if cLastTerm == lastLog.Term {
		return cLastIdx >= lastLog.Index
	}

	return cLastTerm > lastLog.Term
}

// AppendEntries endpoint
// called by the leader of the raft
func (remoteRaft *RemoteRaft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf := remoteRaft.Raft
	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()
	defer rf.persist()

	// init reply
	reply.Term = rf.CurrentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	trace := rf.RTrace.Tracer.ReceiveToken(args.Token)
	trace.RecordAction(ReceiveAppendEntries{args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.SelfIndex, args.Entries})

	reply.Token = trace.GenerateToken()
	if args.Term < rf.CurrentTerm {
		return nil
	}

	if args.Term > rf.CurrentTerm {
		rf.setToFollower(args.Term)
	}

	lastIndex := len(rf.Logs) - 1
	rf.HbCh <- HBMsg{args.Term, args.LeaderId}

	// follower's log is shorter than the leader
	if args.PrevLogIndex > lastIndex {
		reply.ConflictIndex = lastIndex + 1
		return nil
	}

	// check if the term matches
	if cfTerm := rf.Logs[args.PrevLogIndex].Term; cfTerm != args.PrevLogTerm {
		reply.ConflictTerm = cfTerm
		for i := args.PrevLogIndex; i >= 0 && rf.Logs[i].Term == cfTerm; i-- {
			reply.ConflictIndex = i
		}
		reply.Success = false
		return nil
	}

	// check the args.Entries to see if it matches with rf.Logs
	// break when it finds the first
	i, j := args.PrevLogIndex+1, 0
	for ; i <= lastIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.Logs[i].Term != args.Entries[j].Term {
			break
		}
	}

	// truncate the log, and use args.Entries to fill in the log
	// to keep log consistency
	rf.Logs = rf.Logs[:i]
	args.Entries = args.Entries[j:]
	rf.Logs = append(rf.Logs, args.Entries...)

	reply.Success = true

	// update commit index to min(leaderCommit, lastIndex)
	if args.LeaderCommit > rf.CommitIndex {
		lastIndex = len(rf.Logs) - 1
		if args.LeaderCommit < lastIndex {
			rf.CommitIndex = args.LeaderCommit
		} else {
			rf.CommitIndex = lastIndex
		}

		go rf.apply()
	}

	rf.CurrLeaderIndex = args.LeaderId

	return nil

}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() RaftState {
	var term int
	var isLeader bool
	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()
	fmt.Printf("current term of %v: %v \n", rf.SelfIndex, rf.CurrentTerm)
	term = rf.CurrentTerm
	isLeader = rf.Identity == LEADER
	return RaftState{len(rf.Logs) - 1, term, isLeader, rf.CurrLeaderIndex}
}

// Execute a command, called when the server gets a request
func (rf *Raft) Execute(command util.RaftPutReq, reqToken tracing.TracingToken) tracing.TracingToken {
	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()

	reqTrace := rf.RTrace.Tracer.ReceiveToken(reqToken)
	reqTrace.RecordAction(ExecuteCommand{rf.CurrentTerm, rf.CurrLeaderIndex, command})
	rf.Logs = append(rf.Logs, LogEntry{command, rf.CurrentTerm, len(rf.Logs)})
	rf.persist()

	return reqTrace.GenerateToken()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) persist() {
	gob.Register(util.RaftPutReq{})
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	pd := PersistData{rf.CurrentTerm, rf.VotedFor, rf.Logs}
	if err := e.Encode(pd); err != nil {
		fmt.Printf("Error in persist encoding\n\n\n")
		fmt.Printf("%v \n\n\n", err)
		return
	}
	data := w.Bytes()
	if bytes.Equal(data, rf.Persister.GetRaftState()) {
		return
	}
	rf.Persister.SaveRaftState(data)
	rf.Persister.Persist(rf.SelfIndex)
	rf.RTrace.RecordAction(pd)
}

//
// restore previously persisted state.
// can be left to m2/m3
//
func (rf *Raft) readPersist() {

	err := rf.Persister.ReadPersist(rf.SelfIndex)
	if err != nil {
		return
	}
	data := rf.Persister.GetRaftState()

	// check whether the data is empty
	if data == nil || len(data) < 1 {
		return
	}

	gob.Register(util.PutArgs{})
	gob.Register(util.GetArgs{})
	gob.Register(util.RaftPutReq{})

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	var d PersistData
	if err := dec.Decode(&d); err != nil {
		fmt.Printf("error decoding log file data \n\n")
		fmt.Printf("%v \n\n", err)
		return
	}

	rf.CurrentTerm = d.CurrentTerm
	rf.VotedFor = d.VotedFor
	rf.Logs = d.Logs

	rf.RTrace.RecordAction(ReadPersist{rf.CurrentTerm, rf.VotedFor, rf.Logs})
}

// broadcast request vote requests to all Peers
// must be called after lock is held
func (rf *Raft) broadcastRequestVote() {
	for i := range rf.Peers {
		if i == rf.SelfIndex {
			continue
		}
		args := &RequestVoteArgs{}
		args.Term = rf.CurrentTerm
		args.CandidateId = rf.SelfIndex
		args.LastLogIndex = len(rf.Logs) - 1
		args.LastLogTerm = rf.Logs[args.LastLogIndex].Term
		reply := &RequestVoteReply{}

		go rf.sendRequestVote(i, args, reply)
	}
}

//
// send a RequestVote RPC to a server.
// server is the index of the target server in rf.Peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
func (rf *Raft) sendRequestVote(serverIdx int, args *RequestVoteArgs, reply *RequestVoteReply) {
	trace := rf.RTrace.Tracer.CreateTrace()
	trace.RecordAction(SendRequestVote{args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.SelfIndex})
	args.Token = trace.GenerateToken()
	err := rf.Peers[serverIdx].Call("Raft.RequestVote", args, reply)
	if err != nil {
		fmt.Printf("error in rpc call server from %v to %v: %v \n", rf.SelfIndex, serverIdx, err)
		return
	}

	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()

	trace = rf.RTrace.Tracer.ReceiveToken(reply.Token)
	trace.RecordAction(RequestVoteRes{reply.Term, reply.VoteGranted, serverIdx, rf.SelfIndex})

	if rf.Identity != CANDIDATE || args.Term != rf.CurrentTerm || reply.Term < rf.CurrentTerm {
		return
	}

	if reply.Term > rf.CurrentTerm {
		rf.setToFollower(reply.Term)
		rf.persist()
		return
	}

	if reply.VoteGranted {
		rf.VoteCount++
		if rf.VoteCount >= len(rf.Peers)/2+1 {
			rf.WinElectCh <- true
			trace.RecordAction(LeaderElected{rf.CurrentTerm, rf.SelfIndex})
		}
	}

}

// broadcast appendEntries requests to all Peers
// must be called after lock is held
func (rf *Raft) broadcastAppendEntries() {
	if rf.Identity != LEADER {
		return
	}

	reply := &AppendEntriesReply{}

	for i := range rf.Peers {
		if i == rf.SelfIndex {
			continue
		}
		args := &AppendEntriesArgs{}
		args.LeaderId = rf.SelfIndex
		args.LeaderCommit = rf.CommitIndex
		args.Term = rf.CurrentTerm

		args.PrevLogIndex = rf.NextIndex[i] - 1
		args.PrevLogTerm = rf.Logs[args.PrevLogIndex].Term

		// send all entries after rf.NextIndex[i]
		entries := rf.Logs[rf.NextIndex[i]:]
		args.Entries = make([]LogEntry, len(entries))
		copy(args.Entries, entries)

		go rf.sendAppendEntries(i, args, reply)
	}
}

func (rf *Raft) sendAppendEntries(serverIdx int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	trace := rf.RTrace.Tracer.CreateTrace()
	trace.RecordAction(SendAppendEntries{args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.SelfIndex, args.Entries})
	args.Token = trace.GenerateToken()
	err := rf.Peers[serverIdx].Call("Raft.AppendEntries", args, reply)
	if err != nil {
		fmt.Printf("error in rpc call server from %v to %v: %v \n", rf.SelfIndex, serverIdx, err)
		return
	}

	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()
	defer rf.persist()

	trace = trace.Tracer.ReceiveToken(reply.Token)
	trace.RecordAction(AppendEntriesRes{reply.Term, reply.Success, reply.PrevLogIndex, reply.ConflictTerm, reply.ConflictIndex})
	if rf.Identity != LEADER || args.Term != rf.CurrentTerm || reply.Term < rf.CurrentTerm {
		return
	}

	if reply.Term > rf.CurrentTerm {
		rf.setToFollower(args.Term)
		return
	}

	// update MatchIndex and NextIndex of followers
	if reply.Success {
		newMatchIdx := args.PrevLogIndex + len(args.Entries)
		if newMatchIdx > rf.MatchIndex[serverIdx] {
			rf.MatchIndex[serverIdx] = newMatchIdx
		}

		rf.NextIndex[serverIdx] = newMatchIdx + 1
	} else if reply.ConflictTerm < 0 {
		// follower's log shorter than leader's log
		rf.NextIndex[serverIdx] = reply.ConflictIndex
		rf.MatchIndex[serverIdx] = reply.ConflictIndex - 1
	} else {
		// find the conflict term in log
		newNextIndex := len(rf.Logs) - 1
		for ; newNextIndex >= 0; newNextIndex-- {
			if rf.Logs[newNextIndex].Term == reply.ConflictTerm {
				break
			}
		}

		// if not found, set next index to conflict index
		if newNextIndex < 0 {
			rf.NextIndex[serverIdx] = reply.ConflictIndex
		} else {
			rf.NextIndex[serverIdx] = newNextIndex
		}

		rf.MatchIndex[serverIdx] = newNextIndex - 1
	}

	rf.Commit(trace)
}

// if there's an idx i where i >= rf.CommitIndex and
// for majority Peers, the matchIdx of that peer >= i (it's sent to more than majority of Peers)
// update commitIdx to i and apply
func (rf *Raft) Commit(trace *tracing.Trace) {
	for i := len(rf.Logs) - 1; i >= rf.CommitIndex; i-- {
		sentCount := 1 // count itself

		for j := range rf.Peers {
			if j == rf.SelfIndex {
				continue
			}
			if rf.MatchIndex[j] >= i {
				sentCount++
			}
		}

		if sentCount > len(rf.Peers)/2 {
			rf.CommitIndex = i
			trace.RecordAction(Commit{rf.SelfIndex, rf.CurrentTerm, i})
			go rf.apply()
			break // find the latest idx that hasn't committed but already sent to majority
		}
	}
}

func (rf *Raft) setToFollower(term int) {
	ident := rf.Identity
	rf.Identity = FOLLOWER
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.VoteCount = 0

	if ident != FOLLOWER {
		rf.StepDownCh <- true
	}
}

// set raft state to candidate
func (rf *Raft) setToCandidate(identityType IdentityType) {
	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()

	// avoid data racing
	if rf.Identity != identityType {
		return
	}
	rf.resetChannels()
	rf.Identity = CANDIDATE
	rf.CurrentTerm++
	rf.VotedFor = rf.SelfIndex
	rf.VoteCount = 1

	rf.persist()
	rf.broadcastRequestVote()
}

func (rf *Raft) setToLeader() {
	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()
	if rf.Identity != CANDIDATE {
		return
	}

	rf.resetChannels()
	rf.Identity = LEADER
	rf.CurrLeaderIndex = rf.SelfIndex
	rf.NextIndex = make([]int, len(rf.Peers))
	rf.MatchIndex = make([]int, len(rf.Peers))

	// init to the index of last log + 1: len(rf.Logs) -1 + 1
	lastIndex := len(rf.Logs)
	for i := range rf.Peers {
		rf.NextIndex[i] = lastIndex
	}

	rf.broadcastAppendEntries()
}

// apply the Logs entries that has committed
func (rf *Raft) apply() {
	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()
	for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs[i].Command,
			CommandIndex: i,
		}
		rf.ApplyCh <- applyMsg
		rf.RTrace.RecordAction(Apply{rf.SelfIndex, rf.CurrentTerm, applyMsg.CommandValid, applyMsg.Command, applyMsg.CommandIndex})
		rf.LastApplied = i
	}
}

// provide a random timeout value
// for raft it's normally between 150ms - 300ms
func randomTimeout(min, max int) int {
	return rand.Intn(max-min) + min
}

func (rf *Raft) Kill() {
	rf.Mutex.Lock()
	defer rf.Mutex.Unlock()
	rf.Dead = true
	rf.RTrace.RecordAction(RaftEnd{rf.SelfIndex})
}

// init and start a raft instance
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in Peers[]. this
// server's port is Peers[SelfIndex]. all the servers' Peers[] arrays
// have the same order. Persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. ApplyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Start() must return quickly, so it should start goroutines
// for any long-running work.
// HbCh is the channel for hb msg, the server should setup listener and push
// hb into this channel
//
func StartRaft(peers []*util.RPCEndPoint, selfidx int,
	persister *util.Persister, applyCh chan ApplyMsg, tracer *tracing.Tracer) (*Raft, error) {
	rf := &Raft{}
	rf.Dead = false
	rf.Peers = peers
	rf.PeersLen = len(peers)
	rf.SelfIndex = selfidx
	rf.CurrLeaderIndex = -1
	rf.ApplyCh = applyCh

	rf.VotedFor = -1
	rf.Logs = append(rf.Logs, LogEntry{
		Command: "Start",
		Term:    0,
		Index:   0,
	})

	rf.RTrace = tracer.CreateTrace()

	// read persistent data from the disk
	rf.Persister = persister
	rf.readPersist()

	rf.setToFollower(rf.CurrentTerm)
	rf.NextIndex = make([]int, rf.PeersLen)
	rf.MatchIndex = make([]int, rf.PeersLen)
	rf.HbCh = make(chan HBMsg, rf.PeersLen)

	rf.resetChannels()

	rand.Seed(time.Now().UnixNano())

	gob.Register(util.GetArgs{})
	gob.Register(util.PutArgs{})
	gob.Register(util.RaftPutReq{})

	rf.RTrace.RecordAction(RaftStart{rf.SelfIndex})

	_, err := util.StartRPCListener(rf.Peers[selfidx].Addr)
	if err != nil {
		fmt.Printf("listener error: %v \n", err)
		return nil, err
	}

	remoteRaft := &RemoteRaft{Raft: rf}
	rpc.RegisterName("Raft", remoteRaft)
	// raft process called in a goroutine to keep running in the background
	go rf.runRaft()
	return rf, nil
}

func (rf *Raft) runRaft() {
	for !rf.Dead {
		rf.Mutex.Lock()
		switch rf.Identity {
		case FOLLOWER:
			rf.Mutex.Unlock()
			select {
			case <-rf.HbCh:
			case <-rf.VoteCh:
			case <-time.After(time.Duration(randomTimeout(800, 1000)) * time.Millisecond):
				rf.setToCandidate(FOLLOWER)
			}
		case CANDIDATE:
			rf.Mutex.Unlock()
			select {
			case <-rf.StepDownCh:
			// set to follower will push to stepdown channel
			// if it's this case then it's already a follower
			case <-rf.WinElectCh:
				rf.setToLeader()
			case <-time.After(time.Duration(randomTimeout(800, 1000)) * time.Millisecond):
				rf.setToCandidate(CANDIDATE)
			}
		case LEADER:
			rf.Mutex.Unlock()
			select {
			case <-rf.StepDownCh:
			// same as above
			case <-time.After(400 * time.Millisecond):
				rf.Mutex.Lock()
				rf.broadcastAppendEntries()
				rf.Mutex.Unlock()
			}
		}
	}
}
