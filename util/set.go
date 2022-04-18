package util

import "sync"

// SafeUInt8Set is a concurrency-safe set data structure for uint8 items.
type SafeUInt8Set struct {
	set   map[uint8]struct{}
	mutex sync.RWMutex
}

func NewSafeUInt8Set() *SafeUInt8Set {
	return &SafeUInt8Set{
		set: make(map[uint8]struct{}),
	}
}

func (s *SafeUInt8Set) Add(value uint8) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.set[value] = struct{}{}
}

func (s *SafeUInt8Set) Remove(value uint8) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.set, value)
}

func (s *SafeUInt8Set) Has(value uint8) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	_, ok := s.set[value]
	return ok
}

func (s *SafeUInt8Set) Size() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return len(s.set)
}

func (s *SafeUInt8Set) IsEmpty() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.Size() == 0
}
