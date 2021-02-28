package raft

import (
	"../utils"
	"sort"
	"sync"
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	IsOldTerm bool // TODO: 删除该字段
	XTerm     int
	XIndex    int
	XLen      int
}

func (rf *Raft) updateCommitIndex() bool {
	matchIndex := make([]int, rf.serverNum)
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	start := matchIndex[(rf.serverNum-1)/2]
	end := rf.commitIndex
	for i := start; i > end; i-- {
		if rf.log[i].Term == rf.currentTerm {
			rf.commitIndex = i
			DPrintf(4, "me: [%d], currentTerm [%d], update commitIndex %v\n", rf.me, rf.currentTerm, rf.commitIndex)
			return true
		}
	}
	return false
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	reply.IsOldTerm = false

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.IsOldTerm = true
		return
	}

	if rf.leaderId == args.LeaderId {
		// TODO: why
		rf.resetElectionTimeout()
	} else {
		rf.leaderId = args.LeaderId
	}
	rf.maybeNewTermReset(args.Term)
	reply.Term = rf.currentTerm

	//DPrintf(2, "me: [%d], currentTerm [%d], follower log %v\n", rf.me, rf.currentTerm, rf.log)

	if len(rf.log)-1 < args.PrevLogIndex {
		reply.XTerm = -1
		reply.XLen = args.PrevLogIndex - len(rf.log) + 1
		rf.mu.Unlock()
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		var i int
		for i = args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != reply.XTerm {
				break
			}
		}
		reply.XIndex = i + 1
		rf.mu.Unlock()
		return
	}

	defer func() {
		oldCommitIndex := rf.commitIndex
		//DPrintf(2, "me: [%d], currentTerm [%d], oldCommitIndex %v\n", rf.me, rf.currentTerm, oldCommitIndex)
		//DPrintf(2, "me: [%d], currentTerm [%d], len(rf.log) %v\n", rf.me, rf.currentTerm, len(rf.log))
		if args.LeaderCommit > oldCommitIndex {
			rf.commitIndex = utils.Min(args.LeaderCommit, len(rf.log)-1)
		}
		newCommitIndex := rf.commitIndex
		rf.mu.Unlock()
		if newCommitIndex > oldCommitIndex {
			go rf.applyCommit(newCommitIndex)
		}
	}()

	reply.Success = true

	if args.Entries != nil {
		DPrintf(4, "me: [%d], currentTerm [%d], get new entries %v\n", rf.me, rf.currentTerm, args.Entries)
		oriLogLen := len(rf.log)
		newLogLen := len(args.Entries)
		var i, j int
		for i, j = args.PrevLogIndex+1, 0; i < oriLogLen && j < newLogLen; i, j = i+1, j+1 {
			if rf.log[i].Term != args.Entries[j].Term {
				break
			}
		}

		if j == newLogLen {
			return
		}

		rf.log = rf.log[:i]
		rf.log = append(rf.log, args.Entries[j:]...)
		rf.persist()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.sendRPC("Raft.AppendEntries", server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && rf.state == LEADER {
		if reply.Success {
			entries := args.Entries
			if entries != nil {
				rf.nextIndex[server] = utils.Max(rf.nextIndex[server], entries[len(entries)-1].Index+1)
				rf.matchIndex[server] = utils.Max(rf.matchIndex[server], entries[len(entries)-1].Index) // TODO
				//DPrintf(2, "me: [%d], currentTerm [%d], update nextIndex %v\n", rf.me, rf.currentTerm, rf.nextIndex)
				DPrintf(4, "me: [%d], currentTerm [%d], update matchIndex[%d] %v\n", rf.me, rf.currentTerm, server, rf.matchIndex[server])
			}
		} else if !reply.IsOldTerm {
			// 失败原因不是由于 old term 引起的
			// 该判断大部分会被阻止在 sendRPC 函数中，但无法保证全部阻止

			//rf.nextIndex[server]--

			// 优化
			if reply.XTerm == -1 {
				rf.nextIndex[server] -= reply.XLen
			} else {
				if rf.log[reply.XIndex].Term != reply.XTerm {
					rf.nextIndex[server] = reply.XIndex
				} else {
					rf.nextIndex[server] = reply.XIndex + 1
				}
			}

			// TODO: bug
			if rf.nextIndex[server] <= 0 {
				DPrintf(3, "me: [%d], currentTerm [%d], update nextIndex[%v] %v, args %v, reply %v\n", rf.me, rf.currentTerm, server, rf.nextIndex[server], args, reply)
				rf.nextIndex[server] = len(rf.log)
			}
		}
	}
	return ok
}

func (rf *Raft) sendHeartbeatToAServer(server int, term int, done *sync.WaitGroup) {
	defer done.Done()

	for !rf.killed() {
		rf.mu.Lock()
		// 当 state 变为 follower 后，最迟在 HEARTBEAT_INTERVAL_MS 后所有心跳线程全部退出
		if rf.currentTerm != term || rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		oldCommitIndex := rf.commitIndex
		if rf.commitIndex < len(rf.log)-1 {
			rf.updateCommitIndex()
		}
		newCommitIndex := rf.commitIndex

		//DPrintf(2, "me: [%d], currentTerm [%d], send rf.commitIndex %v\n", rf.me, rf.currentTerm, newCommitIndex)

		nextIndex := rf.nextIndex[server]
		prevLogIndex := nextIndex - 1
		//DPrintf(2, "me: [%d], currentTerm [%d], leader log %v, prevLogIndex %v\n", rf.me, rf.currentTerm, rf.log, prevLogIndex)
		var entries []Log = nil
		if nextIndex < len(rf.log) {
			//entries = rf.log[nextIndex:]
			entries = append(entries, rf.log[nextIndex:]...)
		}
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  rf.log[prevLogIndex].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		if newCommitIndex > oldCommitIndex {
			//DPrintf(2, "me: [%d], currentTerm [%d], will apply commit %v\n", rf.me, rf.currentTerm, newCommitIndex)
			go rf.applyCommit(newCommitIndex)
		}

		//DPrintf(1, "me: [%d], currentTerm [%d], state [%d]: will send heart beat to [%d]\n", rf.me, rf.currentTerm, rf.state, server)
		reply := AppendEntriesReply{}
		go rf.sendAppendEntries(server, &args, &reply)

		time.Sleep(HEARTBEAT_INTERVAL_MS * time.Millisecond)
	}
}

func (rf *Raft) heartbeat(term int) {
	var done sync.WaitGroup
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		done.Add(1)
		go rf.sendHeartbeatToAServer(i, term, &done)
	}
	done.Wait()
}
