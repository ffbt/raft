package raft

import "time"
import "../utils"

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

	rf.maybeNewTermReset(args.Term)
	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionTimeout()
		DPrintf(1, "me: [%d], currentTerm [%d]: grant vote to [%d] [success]\n", rf.me, rf.currentTerm, args.CandidateId)
	} else {
		DPrintf(1, "me: [%d], currentTerm [%d]: grant vote to [%d] [fail]\n", rf.me, rf.currentTerm, args.CandidateId)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.sendRPC("Raft.RequestVote", server, args, reply)
}

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
		if rf.sendRequestVote(server, &args, &reply) {
			if reply.Term != term || !reply.VoteGranted {
				ch <- 0
			} else {
				ch <- 1
			}
			break
		}
		utils.ShortSleep()
	}
}

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
			return false
		}

		select {
		case x := <-ch:
			responseNum++
			switch x {
			case -1:
				return false
			case 1:
				voteNum++
				if voteNum*2 > rf.serverNum {
					return true
				}
			case 0:
				// continue
			}
		case <-time.After(sleepTime): // 超时
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
			continue
		}

		// 超时，发起 leader 选举
		rf.currentTerm++
		term := rf.currentTerm
		rf.votedFor = rf.me
		rf.state = CANDIDATE
		rf.resetElectionTimeout()
		rf.persist()
		DPrintf(1, "me: [%d], currentTerm [%d]: timeout and become candidate\n", rf.me, rf.currentTerm)
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
		rf.state = LEADER
		rf.nextIndex = make([]int, rf.serverNum)
		for i := 0; i < rf.serverNum; i++ {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
		}
		rf.matchIndex = make([]int, rf.serverNum)
		for i := 0; i < rf.serverNum; i++ {
			rf.matchIndex[i] = 0
		}
		DPrintf(4, "me: [%d], currentTerm [%d]: become leader\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()

		rf.heartbeat(term)
	}
}
