package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
	"../utils"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	serverNum    int
	cachedLeader int
	clientID     int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.serverNum = len(servers)
	ck.cachedLeader = int(nrand()) % ck.serverNum
	ck.clientID = time.Now().UnixNano()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	DPrintf(2, "get key %v\n", key)
	args := GetArgs{
		key,
		ck.clientID,
		time.Now().UnixNano(),
	}
	for {
		for i := ck.cachedLeader; i < ck.cachedLeader+ck.serverNum; i++ {
			l := i % ck.serverNum
			ch := make(chan bool)
			reply := GetReply{}
			go func() {
				ok := ck.servers[l].Call("KVServer.Get", &args, &reply)
				ch <- ok
			}()
			select {
			case ok := <-ch:
				if ok {
					//DPrintf(1, "get reply %v\n", reply)
					switch reply.Err {
					case OK:
						ck.cachedLeader = l
						return reply.Value
					case ErrNoKey:
						ck.cachedLeader = l
						return ""
					}
				}
			case <-time.After(utils.GetMiddleTime()):
				// 超时
			}
		}
		utils.ShortSleep()
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf(2, "%v key %v, value %v\n", op, key, value)
	args := PutAppendArgs{
		key,
		value,
		op,
		ck.clientID,
		time.Now().UnixNano(),
	}
	for {
		DPrintf(1, "iter\n")
		for i := ck.cachedLeader; i < ck.cachedLeader+ck.serverNum; i++ {
			l := i % ck.serverNum
			ch := make(chan bool)
			reply := PutAppendReply{}
			go func() {
				ok := ck.servers[l].Call("KVServer.PutAppend", &args, &reply)
				ch <- ok
			}()
			select {
			case ok := <-ch:
				if ok {
					DPrintf(1, "client get reply %v\n", reply)
					ck.cachedLeader = l
					switch reply.Err {
					case OK:
						return
					}
				}
			case <-time.After(utils.GetMiddleTime()):
				DPrintf(1, "time out\n")
				// 超时
			}
		}
		utils.ShortSleep()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
	//DPrintf(1, "Put done\n")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
	//DPrintf(1, "Append done\n")
}
