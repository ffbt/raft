package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (Index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"../labrpc"
	"bytes"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Index   int
	Term    int
	Command interface{}
}

type ServerState int

const (
	FOLLOWER ServerState = iota
	CANDIDATE
	LEADER
)

const ELECTION_TIMEOUT_MS_LOW = 500
const ELECTION_TIMEOUT_MS_HIGH = 1000
const HEARTBEAT_INTERVAL_MS = 100

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a descr

	// const
	serverNum int
	applyCh   chan ApplyMsg
	commitCh  chan int

	currentTerm int
	votedFor    int // 初始时为 -1，每个 term 最多只能给一个 server 投票，即投给最先来的 server
	state       ServerState
	leaderId    int
	log         []Log
	commitIndex int
	lastApplied int

	// 仅在非 leader 状态下使用
	electionTimeout time.Time

	// 仅在成为 leader 后使用
	nextIndex  []int
	matchIndex []int
}

func getRandomSleepTime() time.Duration {
	return time.Duration((rand.Float64()*(ELECTION_TIMEOUT_MS_HIGH-ELECTION_TIMEOUT_MS_LOW) + ELECTION_TIMEOUT_MS_LOW) * float64(time.Millisecond))
}

func (rf *Raft) resetElectionTimeout() {
	sleepTime := getRandomSleepTime()
	rf.electionTimeout = time.Now().Add(sleepTime)
}

func (rf *Raft) maybeNewTermReset(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.leaderId = -1
		DPrintf(1, "me: [%d], currentTerm [%d]: become follower\n", rf.me, rf.currentTerm)
		rf.persist()
	}
}

//
// example code to send a sendRPC RPC to a server.
// server is the Index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// 返回 true 表示继续处理，否则丢弃
//
func (rf *Raft) sendRPC(svcMeth string, server int, args interface{}, reply interface{}) bool {
	ok := rf.peers[server].Call(svcMeth, args, reply)
	if ok {
		term := int(reflect.ValueOf(reply).Elem().Field(0).Int())
		rf.mu.Lock()
		if rf.currentTerm != term {
			rf.maybeNewTermReset(term)
			ok = false
		}
		rf.mu.Unlock()
	}
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	DPrintf(3, "me: [%d], currentTerm [%d], encode currentTerm %v, votedFor %v, log %v\n", rf.me, rf.currentTerm, rf.currentTerm, rf.votedFor, rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	d.Decode(&currentTerm)
	d.Decode(&votedFor)
	d.Decode(&log)
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	DPrintf(3, "me: [%d], currentTerm [%d], decode currentTerm %v, votedFor %v, log %v\n", rf.me, rf.currentTerm, rf.currentTerm, rf.votedFor, rf.log)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = (rf.state == LEADER)
	if !isLeader {
		rf.mu.Unlock()
	} else {
		//DPrintf(2, "me: [%d], currentTerm [%d], get command %v\n", rf.me, rf.currentTerm, command)
		index = rf.nextIndex[rf.me]
		term = rf.currentTerm
		log := Log{
			Index:   index,
			Term:    term,
			Command: command,
		}
		//DPrintf(2, "me: [%d], currentTerm [%d], append log %v\n", rf.me, rf.currentTerm, log)
		rf.log = append(rf.log, log)
		rf.persist()
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.mu.Unlock()
		// 需要尽快返回
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//rand.Seed(time.Now().Unix())
	rf.serverNum = len(peers)
	rf.applyCh = applyCh
	rf.commitCh = make(chan int)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.leaderId = -1
	rf.log = append(rf.log, Log{
		0,
		0,
		nil,
	})
	rf.resetElectionTimeout()
	rf.commitIndex, rf.lastApplied = 0, 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.election()
	go rf.apply()

	return rf
}
