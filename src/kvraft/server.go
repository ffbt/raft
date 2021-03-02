package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
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
	ClientID  int64
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
	clientRequestID map[int64]int64          // clientID -> requestID 解决请求的重复性问题
	clientChannel   map[int]chan ApplyResult // cmd index -> client channel 解决响应时请求丢失的问题
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	_, find := kv.clientRequestID[args.ClientID]
	if !find {
		kv.clientRequestID[args.ClientID] = 0
	}
	kv.mu.Unlock()

	cmd := Op{
		Key:       args.Key,
		OpType:    GET,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	index, _, ok := kv.rf.Start(cmd)
	if ok {
		ch := make(chan ApplyResult)
		kv.mu.Lock()
		kv.clientChannel[index] = ch
		kv.mu.Unlock()

		result := <-ch
		if result.ok {
			reply.Value = result.value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}

		kv.mu.Lock()
		delete(kv.clientChannel, index)
		kv.mu.Unlock()
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	_, find := kv.clientRequestID[args.ClientID]
	if !find {
		kv.clientRequestID[args.ClientID] = 0
	}
	kv.mu.Unlock()

	cmd := Op{
		args.Key,
		args.Value,
		args.Op,
		args.ClientID,
		args.RequestID,
	}

	index, _, ok := kv.rf.Start(cmd)
	if ok {
		ch := make(chan ApplyResult)
		kv.mu.Lock()
		kv.clientChannel[index] = ch
		kv.mu.Unlock()

		DPrintf(2, "me: [%d], PutAppend %v\n", kv.me, args)
		<-ch
		DPrintf(1, "me: [%d], PutAppend %v OK\n", kv.me, args)
		reply.Err = OK

		kv.mu.Lock()
		delete(kv.clientChannel, index)
		kv.mu.Unlock()
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

func (kv *KVServer) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.clientRequestID)
	return w.Bytes()
}

func (kv *KVServer) apply() {
	for m := range kv.applyCh {
		// leader 和 follower 都会进入该循环
		if m.CommandValid == false {
			// ignore other types of ApplyMsg
		} else {
			if m.ReadSnapshot {
				// 只有 follower 会进入
				DPrintf(2, "me [%d], apply snapshot\n", kv.me)
				snapshot := kv.rf.ReadSnapshot()
				r := bytes.NewBuffer(snapshot)
				d := labgob.NewDecoder(r)
				db := make(map[string]string)
				cmds := make(map[int64]int64)
				d.Decode(&db)
				d.Decode(&cmds)
				kv.mu.Lock()
				kv.db = db
				kv.clientRequestID = cmds
				kv.mu.Unlock()
				continue
			}

			v := m.Command.(Op)
			DPrintf(1, "me [%d], apply cmd: %v\n", kv.me, v)

			var value string
			ok := true
			kv.mu.Lock()
			switch v.OpType {
			case GET:
				value, ok = kv.db[v.Key]
			case PUT:
				if v.RequestID > kv.clientRequestID[v.ClientID] {
					kv.clientRequestID[v.ClientID] = v.RequestID
					kv.db[v.Key] = v.Value
				}
			case APPEND:
				if v.RequestID > kv.clientRequestID[v.ClientID] {
					kv.clientRequestID[v.ClientID] = v.RequestID
					_, find := kv.db[v.Key]
					if find {
						kv.db[v.Key] += v.Value
					} else {
						kv.db[v.Key] = v.Value
					}
				}
			}
			kv.mu.Unlock()

			_, isLeader := kv.rf.GetState()
			if isLeader {
				// 只需要 leader 执行
				result := ApplyResult{
					value,
					ok,
				}
				kv.mu.Lock()
				ch, find := kv.clientChannel[m.CommandIndex]
				kv.mu.Unlock()
				if find {
					go func() {
						ch <- result
					}()
				}
				if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
					DPrintf(2, "me [%d], create snapshot\n", kv.me)
					kv.mu.Lock()
					snapshot := kv.encodeSnapshot()
					kv.mu.Unlock()
					kv.rf.Snapshot(m.CommandIndex, snapshot)
				}
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
	kv.clientRequestID = make(map[int64]int64)
	kv.clientChannel = make(map[int]chan ApplyResult)
	go kv.apply()

	return kv
}
