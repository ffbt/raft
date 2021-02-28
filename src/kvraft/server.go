package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 2

func DPrintf(debug int, format string, a ...interface{}) (n int, err error) {
	if debug > Debug {
		log.Printf(format, a...)
	}
	return
}

type ApplyResult struct {
	value string
	ok    bool
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	OpType    string
	RequestID int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db              map[string]string
	cmds            map[int64]bool
	requestChannels map[int64]chan ApplyResult
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ch := make(chan ApplyResult)
	cmd := Op{
		Key:       args.Key,
		OpType:    GET,
		RequestID: args.RequestID,
	}

	kv.mu.Lock()
	kv.requestChannels[args.RequestID] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.requestChannels, args.RequestID)
		kv.mu.Unlock()
	}()

	_, _, ok := kv.rf.Start(cmd)
	if ok {
		result := <-ch
		if result.ok {
			reply.Value = result.value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ch := make(chan ApplyResult)
	cmd := Op{
		args.Key,
		args.Value,
		args.Op,
		args.RequestID,
	}

	kv.mu.Lock()
	kv.requestChannels[args.RequestID] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.requestChannels, args.RequestID)
		kv.mu.Unlock()
	}()

	_, _, ok := kv.rf.Start(cmd)
	if ok {
		DPrintf(2, "me: [%d], PutAppend %v\n", kv.me, args)
		<-ch
		DPrintf(1, "me: [%d], PutAppend %v OK\n", kv.me, args)
		reply.Err = OK
	} else {
		DPrintf(1, "me: [%d], PutAppend %v ErrWrongLeader\n", kv.me, args)
		reply.Err = ErrWrongLeader
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) apply() {
	for m := range kv.applyCh {
		if m.CommandValid == false {
			// ignore other types of ApplyMsg
		} else {
			v := m.Command.(Op)
			_, exists := kv.cmds[v.RequestID]
			DPrintf(1, "me [%d], apply cmd: %v\n", kv.me, v)
			var value string
			ok := true
			switch v.OpType {
			case GET:
				value, ok = kv.db[v.Key]
			case PUT:
				if !exists {
					kv.db[v.Key] = v.Value
				}
			case APPEND:
				if !exists {
					_, find := kv.db[v.Key]
					if find {
						kv.db[v.Key] += v.Value
					} else {
						kv.db[v.Key] = v.Value
					}
				}
			}
			kv.cmds[v.RequestID] = true
			result := ApplyResult{
				value,
				ok,
			}
			kv.mu.Lock()
			ch, find := kv.requestChannels[v.RequestID]
			kv.mu.Unlock()
			if find {
				// TODO: 线程终止
				go func() {
					ch <- result
				}()
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.cmds = make(map[int64]bool)
	kv.requestChannels = make(map[int64]chan ApplyResult)
	go kv.apply()

	return kv
}
