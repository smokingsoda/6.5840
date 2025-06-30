package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	Name     string
	ClientID string
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
	lk.ck.Put(lk.Name, "", 0)
	lk.ClientID = kvtest.RandValue(8)
	_, _, err := lk.ck.Get(lk.Name)
	if err != rpc.OK {
		panic("failed to add distributed lock")
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, version, err := lk.ck.Get(lk.Name)
		if err == rpc.OK {
			if value == "" {
				err = lk.ck.Put(lk.Name, lk.ClientID, version)
				if err == rpc.OK {
					return
				}
				if err == rpc.ErrMaybe {
					value, _, _ := lk.ck.Get(lk.Name)
					if value == lk.ClientID {
						return
					}
					continue
				}
			}
		}
		if err == rpc.ErrNoKey {
			panic("failed to acquire the lock: not exist")
		}
		<-ticker.C
	}

}

func (lk *Lock) Release() {
	// Your code here
	value, version, _ := lk.ck.Get(lk.Name)
	if lk.ClientID != value {
		panic("failed to release the lock: not requiring it before")
	}
	for {
		err := lk.ck.Put(lk.Name, "", version)
		if err == rpc.ErrVersion {
			// successfully released
			return
		}
		if err == rpc.ErrMaybe {
			// need to check again
			continue
		}
		if err == rpc.OK {
			// successfully released at the very first time
			return
		}
		panic("something wrong")
	}
}
