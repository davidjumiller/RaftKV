package util

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
)

// Persister interface
type Persister struct {
	mu               sync.Mutex
	persistRaftState []byte
}

func MakePersister() *Persister {
	return &Persister{}
}

func (ps *Persister) SaveRaftState(state []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.persistRaftState = state
}

func (ps *Persister) RaftStateSize() int {
	return len(ps.persistRaftState)
}

func (ps *Persister) GetRaftState() []byte {
	return ps.persistRaftState
}

// Persist the state of current persistor
// called in raft.persist, and should be after calls to save
// persist should always append on the file of stable storage instead of overwriting it
func (ps *Persister) Persist(id int) {
	fileName := "persister_" + strconv.Itoa(id) + ".log"
	f, err := os.OpenFile(fileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("file open error %v \n", err)
	}
	defer f.Close()
	if err != nil {
		fmt.Printf("encode error: %v \n", err)
	}
	_, err = f.Write(ps.persistRaftState)
	if err != nil {
		fmt.Printf("writing file error: %v \n", err)
	}
}

func (ps *Persister) ReadPersist(id int) error {
	fileName := "persister_" + strconv.Itoa(id) + ".log"
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		fmt.Printf("reading file error: %v \n", err)
		return err
	}
	ps.SaveRaftState(data)
	return nil
}
