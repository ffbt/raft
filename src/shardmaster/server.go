package shardmaster

import "../raft"
import "../labrpc"
import "sync"
import "../labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
}

func (sm *ShardMaster) addConfig() Config {
	oldConfig := sm.configs[len(sm.configs)-1]
	var config Config
	config.Num = len(sm.configs)
	for i := 0; i < NShards; i++ {
		config.Shards[i] = oldConfig.Shards[i]
	}
	config.Groups = map[int][]string{}
	for k, v := range oldConfig.Groups {
		config.Groups[k] = v
	}
	sm.configs = append(sm.configs, config)
	return config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	_, isLeader := sm.rf.GetState()
	if isLeader {
		config := sm.addConfig()

		for k, v := range args.Servers {
			_, ok := config.Groups[k]
			if ok {
				config.Groups[k] = append(config.Groups[k], v...)
			} else {
				config.Groups[k] = v
			}
		}

		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	_, isLeader := sm.rf.GetState()
	if isLeader {
		config := sm.addConfig()

		for gid := range args.GIDs {
			_, ok := config.Groups[gid]
			if ok {
				delete(config.Groups, gid)
			}
		}

		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	_, isLeader := sm.rf.GetState()
	if isLeader {
		config := sm.addConfig()

		for i := 0; i < NShards; i++ {
			if config.Shards[args.Shard] == args.GID {
				config.Shards[args.Shard] = 0
				break
			}
		}
		config.Shards[args.Shard] = args.GID

		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	_, isLeader := sm.rf.GetState()
	if isLeader {
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
		DPrintf(1, "query num %v, config %v\n", args.Num, reply.Config)
		reply.Err = OK
		reply.WrongLeader = false
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.

	return sm
}
