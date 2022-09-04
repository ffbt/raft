package raft

import (
	"sort"
	"sync"
	"time"

	"../utils"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int // 用于 follower commit
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
	newCommitIndex := matchIndex[(rf.serverNum-1)/2]

	// 前提：log 的最后一项的 term 一定是 currentTerm
	if rf.getLog(newCommitIndex).Term == rf.currentTerm {
		// 该判断非常重要，只有 currentTerm 的 log 超过一半时才可以提交
		// 假设 leader 在 commit 后挂掉，则后续选择的 leader 一定也有该 log，否则不会被选上 (rf.isUpToDate())
		// 详情查看 Figure 8
		rf.commitIndex = newCommitIndex
		return true
	}
	return false

	// start := matchIndex[(rf.serverNum-1)/2]
	// end := rf.commitIndex
	// for i := start; i > end && i >= rf.log[0].Index; i-- {
	// 	if rf.getLog(i).Term == rf.currentTerm {
	// 		rf.commitIndex = i
	// 		DPrintf(4, "me: [%d], currentTerm [%d], update commitIndex %v\n", rf.me, rf.currentTerm, rf.commitIndex)
	// 		return true
	// 	}
	// }
	// return false
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
		// Reply false if term < currentTerm (§5.1)
		reply.Term = rf.currentTerm
		rf.mu.Unlock()

		reply.IsOldTerm = true
		return
	}

	// 任务一：发送心跳
	if rf.leaderId == args.LeaderId {
		// TODO: why
		// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate: convert to candidate
		rf.resetElectionTimeout()
	} else {
		rf.leaderId = args.LeaderId
	}

	// If AppendEntries RPC received from new leader: convert to follower
	rf.maybeNewTermReset(args.Term)
	reply.Term = rf.currentTerm

	find, prevLogLocalIndex := rf.getLogLocalIndex(args.PrevLogIndex)
	if !find || args.PrevLogTerm == -1 {
		// 如果进入，则说明 follower 已经将该部分 snapshot 了
		// 这里不需要做任何动作，leader 等待 follower 对 snapshot 的响应后会修改 nextIndex，进而修改 PrevLogIndex
		// 见 sendInstallSnapshot()
		DPrintf(5, "me: [%d], currentTerm [%d], can't find prevLogIndex %v\n", rf.me, rf.currentTerm, args.PrevLogIndex)
		reply.Success = true
		rf.mu.Unlock()
		return
	}

	//DPrintf(2, "me: [%d], currentTerm [%d], follower log %v\n", rf.me, rf.currentTerm, rf.log)

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// 优化 nextIndex
	if rf.getLastLogIndex() < args.PrevLogIndex {
		// log 中没有 index
		reply.XTerm = -1
		reply.XIndex = rf.getLastLogIndex()
		rf.mu.Unlock()
		return
	}

	if rf.log[prevLogLocalIndex].Term != args.PrevLogTerm {
		// log 中有 index，但 term 不匹配
		reply.XTerm = rf.log[prevLogLocalIndex].Term
		var i int
		for i = prevLogLocalIndex; i >= 0; i-- {
			if rf.log[i].Term != reply.XTerm {
				break
			}
		}
		// 返回 term 的第一个 index
		reply.XIndex = rf.getLogGlobalIndex(i + 1)
		rf.mu.Unlock()
		return
	}
	// log 的 index 和 term 都匹配

	reply.Success = true

	// 任务二：append log entries
	// AppendEntry 不应该是简单的比对完 prevLogIndex 后发现没有冲突，就把 Log append 到 prevLogIndex 后面，
	// 而是要仔细比对，直到出现有冲突的 log，才进行数据的修改。删掉该 index 及往后的 log。这样做的原因是 AppendEntry
	// 可能有旧的 RPC 晚到，直接 append 会导致 LOG 反而变短了，那么就可能导致 commit 后的 Log 并没有出现在大部分的服务器中。
	if args.Entries != nil {
		DPrintf(4, "me: [%d], currentTerm [%d], get new entries %v\n", rf.me, rf.currentTerm, args.Entries)
		oriLogLen := len(rf.log)
		newLogLen := len(args.Entries)
		var i, j int
		for i, j = prevLogLocalIndex+1, 0; i < oriLogLen && j < newLogLen; i, j = i+1, j+1 {
			if rf.log[i].Term != args.Entries[j].Term {
				// 找到第一个不相同的位置
				break
			}
		}

		if j != newLogLen {
			// 将后面不相同的地方截断，把新的 append 到后面
			// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
			rf.log = rf.log[:i]
			// Append any new entries not already in the log
			rf.log = append(rf.log, args.Entries[j:]...)
			rf.persist()
		}
	}

	// 任务三：提示 follower 哪些 log 可以 commit
	oldCommitIndex := rf.commitIndex
	DPrintf(5, "me: [%d], currentTerm [%d], oldCommitIndex %v\n", rf.me, rf.currentTerm, oldCommitIndex)
	DPrintf(5, "me: [%d], currentTerm [%d], lastLogIndex %v\n", rf.me, rf.currentTerm, rf.getLastLogIndex())
	if args.LeaderCommit > oldCommitIndex {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		rf.commitIndex = utils.Min(args.LeaderCommit, rf.getLastLogIndex())
	}
	newCommitIndex := rf.commitIndex
	rf.mu.Unlock()

	if newCommitIndex > oldCommitIndex {
		go rf.applyCommit(newCommitIndex)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.sendRPC("Raft.AppendEntries", server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok && rf.state == LEADER {
		if reply.Success {
			// If successful: update nextIndex and matchIndex for follower (§5.3)
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

			// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)

			//rf.nextIndex[server]--

			// 优化 nextIndex
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XIndex + 1
			} else {
				if reply.XIndex >= rf.log[0].Index {
					if rf.getLog(reply.XIndex).Term != reply.XTerm {
						// index 匹配，term 不匹配
						// preLogIndex = reply.XIndex - 1，向前寻找 term
						rf.nextIndex[server] = reply.XIndex
					} else {
						// index 和 term 都匹配
						// preLogIndex = reply.XIndex
						rf.nextIndex[server] = reply.XIndex + 1
					}
				}
			}
		}
	}
	return ok
}

// 在发送心跳时顺带发送 log
func (rf *Raft) sendHeartbeatToAServer(server int, term int, done *sync.WaitGroup) {
	defer done.Done()

	for !rf.killed() {
		rf.mu.Lock()

		// 当 state 变为 follower 后，最迟在 HEARTBEAT_INTERVAL_MS 后所有心跳线程全部退出
		if rf.currentTerm != term || rf.state != LEADER {
			rf.mu.Unlock()

			return // 执行 done.Done()
		}

		// snapshot
		// 可以将 snapshot 看作与 append log entries 并行的任务
		nextIndex := rf.nextIndex[server]
		if rf.needSnapshot[server] || nextIndex < rf.lastIncludedIndex {
			// 正常情况下执行完以后 nextIndex 更新为 lastIncludedIndex + 1
			// 第二个判断非常重要，目标 server 挂了重新连接以后通过第二个判断可以 install snapshot
			rf.needSnapshot[server] = false
			lastIncludedIndex := rf.lastIncludedIndex
			lastIncludedTerm := rf.lastIncludedTerm
			snapshot := rf.persister.ReadSnapshot() // 读取 leader 的 snapshot
			rf.mu.Unlock()

			go rf.sendInstallSnapshotToAServer(server, lastIncludedIndex, lastIncludedTerm, snapshot)

			// 留下一段时间由上面的 goroutine 更新 nextIndex 和 matchIndex
			time.Sleep(HEARTBEAT_INTERVAL_MS * time.Millisecond)
			continue
		}

		// log entries

		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
		// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
		oldCommitIndex := rf.commitIndex
		if rf.commitIndex < rf.getLastLogIndex() {
			rf.updateCommitIndex()
		}
		newCommitIndex := rf.commitIndex

		//DPrintf(2, "me: [%d], currentTerm [%d], send rf.commitIndex %v\n", rf.me, rf.currentTerm, newCommitIndex)

		nextIndex = rf.nextIndex[server]
		prevLogIndex := nextIndex - 1
		//DPrintf(2, "me: [%d], currentTerm [%d], leader log %v, prevLogIndex %v\n", rf.me, rf.currentTerm, rf.log, prevLogIndex)

		// 由于 snapshot 的原因，log 中并不保存所有的 log，因此区分 localIndex 和 index
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
