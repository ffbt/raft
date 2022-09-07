package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"../labgob"
	"../labrpc"
	"../raft"
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
	clientChannel   map[int]chan ApplyResult // cmd log index -> client channel 为每个 cmd 注册一个 ch，解决响应时请求丢失的问题
}

func (kv *KVServer) recordClient(clientID int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, find := kv.clientRequestID[clientID]
	if !find {
		kv.clientRequestID[clientID] = 0
	}
}

func (kv *KVServer) registerChannel(index int, ch chan ApplyResult) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.clientChannel[index] = ch
}

func (kv *KVServer) unregisterChannel(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.clientChannel, index)
}

// 加快读请求：
// http://codefever.github.io/2019/09/17/raft-linearizable-read/
// https://juejin.cn/post/6906508138124574728
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.recordClient(args.ClientID)

	cmd := Op{
		Key:       args.Key,
		OpType:    GET,
		ClientID:  args.ClientID,
		RequestID: args.RequestID,
	}

	index, _, isLeader := kv.rf.Start(cmd)
	if isLeader {
		ch := make(chan ApplyResult)
		kv.registerChannel(index, ch)

		result := <-ch
		if result.ok {
			reply.Value = result.value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}

		kv.unregisterChannel(index)
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.recordClient(args.ClientID)

	cmd := Op{
		args.Key,
		args.Value,
		args.Op,
		args.ClientID,
		args.RequestID,
	}

	index, _, isLeader := kv.rf.Start(cmd)
	if isLeader {
		ch := make(chan ApplyResult)
		kv.registerChannel(index, ch)

		DPrintf(2, "me: [%d], PutAppend %v\n", kv.me, args)
		<-ch
		DPrintf(2, "me: [%d], PutAppend %v OK\n", kv.me, args)
		reply.Err = OK

		kv.unregisterChannel(index)
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

	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.clientRequestID)
	kv.mu.Unlock()

	return w.Bytes()
}

func (kv *KVServer) readSnapshot() {
	DPrintf(2, "me [%d], apply snapshot\n", kv.me)
	snapshot := kv.rf.ReadSnapshot()
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	db := make(map[string]string)
	clientRequestID := make(map[int64]int64)
	d.Decode(&db)
	d.Decode(&clientRequestID)

	kv.mu.Lock()
	kv.db = db
	kv.clientRequestID = clientRequestID
	kv.mu.Unlock()
}

func (kv *KVServer) apply() {
	for applyMsg := range kv.applyCh {
		// leader 和 follower 都会进入该循环
		if !applyMsg.CommandValid {
			// ignore other types of ApplyMsg
		} else {
			if applyMsg.ReadSnapshot {
				// 只有 follower 会进入
				// 执行到这里可以保证 rf.lastApplied 更新到 rf.lastIncludedIndex，下一次 apply 从下一个 index 开始
				kv.readSnapshot()
				continue
			}

			op := applyMsg.Command.(Op)
			DPrintf(2, "me [%d], apply cmd: %v\n", kv.me, op)

			var value string
			ok := true

			kv.mu.Lock()
			switch op.OpType {
			case GET:
				value, ok = kv.db[op.Key]
			case PUT:
				if op.RequestID > kv.clientRequestID[op.ClientID] {
					kv.clientRequestID[op.ClientID] = op.RequestID
					// 保证是对于 client 最新的结果

					kv.db[op.Key] = op.Value
				}
			case APPEND:
				if op.RequestID > kv.clientRequestID[op.ClientID] {
					kv.clientRequestID[op.ClientID] = op.RequestID

					_, find := kv.db[op.Key]
					if find {
						kv.db[op.Key] += op.Value
					} else {
						kv.db[op.Key] = op.Value
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
				ch, find := kv.clientChannel[applyMsg.CommandIndex]
				kv.mu.Unlock()

				if find {
					go func() {
						ch <- result
					}()
				}

				// need create snapshot?
				if kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate {
					DPrintf(2, "me [%d], create snapshot\n", kv.me)
					snapshot := kv.encodeSnapshot()
					// 使用 applyMsg.CommandIndex 的必要性：必须保证 snapshot 是已经 apply 的
					kv.rf.Snapshot(applyMsg.CommandIndex, snapshot)
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

	kv.db = make(map[string]string)
	kv.clientRequestID = make(map[int64]int64)
	kv.clientChannel = make(map[int]chan ApplyResult)
	go kv.apply()

	return kv
}
