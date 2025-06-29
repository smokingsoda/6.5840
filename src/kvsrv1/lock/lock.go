package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const (
	LOCKED   = "locked"
	UNLOCKED = "unlocked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck          kvtest.IKVClerk
	Name        string
	LockVersion rpc.Tversion
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, Name: l}
	// You may add code here
	lk.ck.Put(lk.Name, UNLOCKED, 0)
	_, version, _ := lk.ck.Get(lk.Name)
	lk.LockVersion = version
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, version, err := lk.ck.Get(lk.Name)
		if err == rpc.OK {
			if value == UNLOCKED {
				err = lk.ck.Put(lk.Name, LOCKED, version)
				if err == rpc.OK {
					lk.LockVersion = version + 1
					return
				}
			}
		}
		<-ticker.C
	}

}

func (lk *Lock) Release() {
	// Your code here
	_, version, _ := lk.ck.Get(lk.Name)
	if lk.LockVersion != version {
		panic("version control failed")
	}
	lk.ck.Put(lk.Name, UNLOCKED, lk.LockVersion)

}
