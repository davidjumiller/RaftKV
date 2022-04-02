package util

import "sync"

// Persister interface
type Persister struct {
	mu               sync.Mutex
	persistRaftState []byte
	persistKVState   []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.persistRaftState = state
}

func (ps *Persister) SaveKVState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.persistKVState = state
}

func (ps *Persister) RaftStateSize() int {
	return len(ps.persistRaftState)
}

func (ps *Persister) KVStateSize() int {
	return len(ps.persistKVState)
}

// Persist the state of current persistor
// called in raft.persist, and should be after calls to save
// persist should always append on the file of stable storage instead of overwriting it
func (ps *Persister) Persist() {

}
