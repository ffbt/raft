package raft

import (
	"time"

	"../utils"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) needSleep() (bool, time.Duration) {
	var isNeedSleep bool
	var sleepTime time.Duration

	now := time.Now().UnixNano()
	future := rf.electionTimeout.UnixNano()
	if future > now { // 没有超时
		isNeedSleep = true
		sleepTime = time.Duration(future-now) * time.Nanosecond
	} else {
		isNeedSleep = false
		sleepTime = 0
	}

	return isNeedSleep, sleepTime
}

func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	// TODO: No cheating and just checking the length!
	// 该判断非常重要
	if rf.getLastLogTerm() == lastLogTerm {
		return lastLogIndex >= rf.getLastLogIndex()
	}
	return lastLogTerm > rf.getLastLogTerm()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.VoteGranted = false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		DPrintf(1, "me: [%d], currentTerm [%d]: grant vote to [%d] [fail], reason: small term\n", rf.me, rf.currentTerm, rf.votedFor)
		return
	}

	// 不能在此无条件执行 rf.resetElectionTimeout()
	// 假设下面流程：
	// 1. 在 term = 1 时 leader1 断开连接，此时 follower2,3,4 正在等待超时
	// 2. follower5 在 term = 0 时接入连接，由于没有 leader，因此假设它首先超时，向 2,3,4 发送投票请求 (参数 term = 1)
	// 3. follower2,3,4 投票失败，follower5 没有成功成为 leader，等待超时
	// 4. follower2,3,4 相互投票，最终某个 follower 成为 leader
	// 若在第 3 步重置超时时间，则 leader 最终选举出来的时间将会延后

	// 如果有一个 term 特别大的突然进来，那么它将称为 leader
	rf.maybeNewTermReset(args.Term)
	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
		rf.resetElectionTimeout()
		DPrintf(1, "me: [%d], currentTerm [%d]: grant vote to [%d] [success]\n", rf.me, rf.currentTerm, args.CandidateId)
	} else {
		DPrintf(1, "me: [%d], currentTerm [%d]: grant vote to [%d] [fail]\n", rf.me, rf.currentTerm, args.CandidateId)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.sendRPC("Raft.RequestVote", server, args, reply)
}

// 若变为 follower，则返回
func (rf *Raft) requestVoteToAServer(server int, term int, ch chan int) {
	rf.mu.Lock()

	if rf.state != CANDIDATE || rf.currentTerm != term {
		rf.mu.Unlock()

		ch <- -1 // request vote 应该停止
		return
	}

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	for !rf.killed() {
		reply := RequestVoteReply{}
		if rf.sendRequestVote(server, &args, &reply) { // 阻塞
			if reply.Term != term || !reply.VoteGranted {
				ch <- 0
			} else {
				ch <- 1
			}
			break
		}

		// rf.sendRequestVote 返回 false
		// 针对 rpc reply.term > rf.currentTerm 的情况
		rf.mu.Lock()
		if rf.state != CANDIDATE || rf.currentTerm != term {
			rf.mu.Unlock()

			ch <- -1 // request vote 应该停止
			return
		}
		rf.mu.Unlock()

		// rpc 发送失败
		utils.ShortSleep()
	}
}

// Follow Figure 2’s directions as to when you should start an election.
// In particular, note that if you are a candidate (i.e., you are currently
// running an election), but the election timer fires, you should start
// another election. This is important to avoid the system stalling due
// to delayed or dropped RPCs.
// 留给该函数的时间只有 election time，超过该时间后进入下一个 term
// 超时后返回 false
func (rf *Raft) requestVote(term int) bool {
	voteNum := 1
	responseNum := 0
	ch := make(chan int, rf.serverNum)

	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.requestVoteToAServer(i, term, ch)
	}

	for !rf.killed() && responseNum < rf.serverNum-1 {
		rf.mu.Lock()
		isNeedSleep, sleepTime := rf.needSleep()
		rf.mu.Unlock()

		if !isNeedSleep {
			// 说明超过了 election time，进入下一个 term
			return false
		}

		select {
		case x := <-ch:
			responseNum++
			switch x {
			case -1:
				// 变为 follower
				return false
			case 1:
				voteNum++
				if voteNum*2 > rf.serverNum {
					// If votes received from majority of servers: become leader
					return true
				}
			case 0:
				// continue
			}
		case <-time.After(sleepTime): // 超时，进入下一个 term
			return false
		}
	}

	return false
}

func (rf *Raft) election() {
	for !rf.killed() {
		rf.mu.Lock()

		isNeedSleep, sleepTime := rf.needSleep()
		if isNeedSleep {
			rf.mu.Unlock()
			time.Sleep(sleepTime)
			continue // 返回到 for
		}

		// 超时，发起 leader 选举
		// On conversion to candidate, start election
		rf.currentTerm++
		term := rf.currentTerm
		rf.votedFor = rf.me
		rf.persist()
		rf.state = CANDIDATE
		rf.resetElectionTimeout()
		DPrintf(7, "me: [%d], currentTerm [%d]: timeout and become candidate\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()

		voted := rf.requestVote(term)
		if !voted {
			// 等待直到超时或其他 server 获得 vote
			continue
		}

		rf.mu.Lock()
		if rf.currentTerm != term {
			rf.mu.Unlock()

			continue
		}

		// 成功在 term 时间段成为 leader
		// 可能也不是具有最新 log 的线程，这里只选了比一半线程 log 新的线程
		rf.state = LEADER
		rf.nextIndex = make([]int, rf.serverNum)
		rf.matchIndex = make([]int, rf.serverNum)
		rf.needSnapshot = make([]bool, rf.serverNum)
		for i := 0; i < rf.serverNum; i++ {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			rf.matchIndex[i] = 0
			rf.needSnapshot[i] = false
		}
		DPrintf(7, "me: [%d], currentTerm [%d]: become leader\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()

		rf.heartbeat(term)
	}
}
