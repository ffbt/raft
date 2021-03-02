package raft

import "../utils"

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int // 不使用，永远为 0
	Data              []byte
	Done              bool // 不使用，永远为 true
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	DPrintf(6, "me: [%d], currentTerm [%d], install snapshot\n", rf.me, rf.currentTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
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

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	commitIndex := args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	find, lastIncludedLocalIndex := rf.getLogLocalIndex(args.LastIncludedIndex)
	if find && lastIncludedLocalIndex < len(rf.log) {
		rf.log = rf.log[lastIncludedLocalIndex:]
	} else {
		rf.log = []LogEntry{}
		rf.log = append(rf.log, LogEntry{
			args.LastIncludedIndex,
			args.LastIncludedTerm,
			nil,
		})
	}

	data := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(data, args.Data)
	DPrintf(7, "me: [%d], currentTerm [%d], log size %v\n", rf.me, rf.currentTerm, rf.persister.RaftStateSize())
	DPrintf(6, "me: [%d], currentTerm [%d], apply commitIndex %v\n", rf.me, rf.currentTerm, commitIndex)
	rf.mu.Unlock()

	go rf.applyCommit(commitIndex)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.sendRPC("Raft.InstallSnapshot", server, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		rf.nextIndex[server] = utils.Max(rf.nextIndex[server], args.LastIncludedIndex+1)
		rf.matchIndex[server] = utils.Max(rf.matchIndex[server], args.LastIncludedIndex)
		DPrintf(5, "me: [%d], currentTerm [%d], update matchIndex[%v] %v\n", rf.me, rf.currentTerm, server, rf.matchIndex[server])
	}

	return ok
}

func (rf *Raft) sendInstallSnapshotToAServer(server int, index int, term int, snapshot []byte) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		index,
		term,
		0,
		snapshot,
		true,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	rf.sendInstallSnapshot(server, &args, &reply)
}

// service -> raft
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	rf.lastIncludedIndex = index
	_, localIndex := rf.getLogLocalIndex(index)
	rf.lastIncludedTerm = rf.log[localIndex].Term
	rf.log = rf.log[localIndex:] // 保留最后一个 log entry
	data := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	for i := 0; i < rf.serverNum; i++ {
		// 后续 follower 的 snapshot 由 heartbeat 发送
		rf.needSnapshot[i] = true
	}
	DPrintf(7, "me: [%d], currentTerm [%d], log size %v\n", rf.me, rf.currentTerm, rf.persister.RaftStateSize())
	rf.mu.Unlock()
}

// service -> raft
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return false
	}
	return false
}
