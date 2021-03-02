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
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	IsOldTerm bool // TODO: 删除该字段
	XTerm     int
	XIndex    int
}

func (rf *Raft) updateCommitIndex() bool {
	matchIndex := make([]int, rf.serverNum)
	copy(matchIndex, rf.matchIndex)
	sort.Ints(matchIndex)
	start := matchIndex[(rf.serverNum-1)/2]
	end := rf.commitIndex
	for i := start; i > end && i >= rf.log[0].Index; i-- {
		if rf.getLog(i).Term == rf.currentTerm {
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
	DPrintf(6, "me: [%d], currentTerm [%d], 233, args %v\n", rf.me, rf.currentTerm, args)
	DPrintf(6, "me: [%d], currentTerm [%d], 233, log %v\n", rf.me, rf.currentTerm, rf.log)
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

	find, prevLogLocalIndex := rf.getLogLocalIndex(args.PrevLogIndex)
	if !find || args.PrevLogTerm == -1 {
		// 如果进入，则说明 follower 已经将该部分 snapshot 了
		// 这里不需要做任何动作，leader 等待 follower 对 snapshot 的响应后会修改 nextIndex，进而修改 PrevLogIndex
		DPrintf(5, "me: [%d], currentTerm [%d], can't find prevLogIndex %v\n", rf.me, rf.currentTerm, args.PrevLogIndex)
		reply.Success = true
		rf.mu.Unlock()
		return
	}

	//DPrintf(2, "me: [%d], currentTerm [%d], follower log %v\n", rf.me, rf.currentTerm, rf.log)

	if rf.getLastLogIndex() < args.PrevLogIndex {
		reply.XTerm = -1
		reply.XIndex = rf.getLastLogIndex()
		rf.mu.Unlock()
		return
	}

	if rf.log[prevLogLocalIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[prevLogLocalIndex].Term
		var i int
		for i = prevLogLocalIndex; i >= 0; i-- {
			if rf.log[i].Term != reply.XTerm {
				break
			}
		}
		reply.XIndex = rf.getLogGlobalIndex(i + 1)
		rf.mu.Unlock()
		return
	}

	defer func() {
		oldCommitIndex := rf.commitIndex
		DPrintf(5, "me: [%d], currentTerm [%d], oldCommitIndex %v\n", rf.me, rf.currentTerm, oldCommitIndex)
		DPrintf(5, "me: [%d], currentTerm [%d], lastLogIndex %v\n", rf.me, rf.currentTerm, rf.getLastLogIndex())
		if args.LeaderCommit > oldCommitIndex {
			rf.commitIndex = utils.Min(args.LeaderCommit, rf.getLastLogIndex())
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
		for i, j = prevLogLocalIndex+1, 0; i < oriLogLen && j < newLogLen; i, j = i+1, j+1 {
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
				rf.matchIndex[server] = utils.Max(rf.matchIndex[server], entries[len(entries)-1].Index)
				//DPrintf(2, "me: [%d], currentTerm [%d], update nextIndex %v\n", rf.me, rf.currentTerm, rf.nextIndex)
				DPrintf(5, "me: [%d], currentTerm [%d], update matchIndex[%d] %v\n", rf.me, rf.currentTerm, server, rf.matchIndex[server])
			}
		} else if !reply.IsOldTerm {
			// 失败原因不是由于 old term 引起的
			// 该判断大部分会被阻止在 sendRPC 函数中，但无法保证全部阻止

			//rf.nextIndex[server]--

			// 优化
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XIndex + 1
			} else {
				if reply.XIndex >= rf.log[0].Index {
					if rf.getLog(reply.XIndex).Term != reply.XTerm {
						rf.nextIndex[server] = reply.XIndex
					} else {
						rf.nextIndex[server] = reply.XIndex + 1
					}
				}
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

		// snapshot
		nextIndex := rf.nextIndex[server]
		if rf.needSnapshot[server] || nextIndex < rf.lastIncludedIndex {
			rf.needSnapshot[server] = false
			lastIncludedIndex := rf.lastIncludedIndex
			lastIncludedTerm := rf.lastIncludedTerm
			snapshot := rf.persister.ReadSnapshot()
			rf.mu.Unlock()

			go rf.sendInstallSnapshotToAServer(server, lastIncludedIndex, lastIncludedTerm, snapshot)
			time.Sleep(HEARTBEAT_INTERVAL_MS * time.Millisecond)
			continue
		}

		// log entries
		oldCommitIndex := rf.commitIndex
		if rf.commitIndex < rf.getLastLogIndex() {
			rf.updateCommitIndex()
		}
		newCommitIndex := rf.commitIndex

		//DPrintf(2, "me: [%d], currentTerm [%d], send rf.commitIndex %v\n", rf.me, rf.currentTerm, newCommitIndex)

		nextIndex = rf.nextIndex[server]
		prevLogIndex := nextIndex - 1
		//DPrintf(2, "me: [%d], currentTerm [%d], leader log %v, prevLogIndex %v\n", rf.me, rf.currentTerm, rf.log, prevLogIndex)

		var prevLogTerm int
		find, prevLogLocalIndex := rf.getLogLocalIndex(prevLogIndex)
		if find {
			prevLogTerm = rf.log[prevLogLocalIndex].Term
		} else {
			prevLogTerm = -1
		}

		var entries []LogEntry = nil
		find, nextLocalIndex := rf.getLogLocalIndex(nextIndex)
		if find {
			entries = append(entries, rf.log[nextLocalIndex:]...)
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		DPrintf(6, "me: [%d], currentTerm [%d], state [%d]: will send heart beat %v to [%d]\n", rf.me, rf.currentTerm, rf.state, args, server)
		rf.mu.Unlock()

		if newCommitIndex > oldCommitIndex {
			//DPrintf(2, "me: [%d], currentTerm [%d], will apply commit %v\n", rf.me, rf.currentTerm, newCommitIndex)
			go rf.applyCommit(newCommitIndex)
		}

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
