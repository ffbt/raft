package raft

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

	if args.LastIncludedIndex >= rf.log[0].Index && args.LastIncludedIndex <= rf.getLastLogIndex() {
		rf.log = rf.log[rf.getLogLocalIndex(args.LastIncludedIndex):]
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
	rf.mu.Unlock()

	go rf.applyCommit(commitIndex)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.sendRPC("Raft.InstallSnapshot", server, args, reply)
}

func (rf *Raft) sendInstallSnapshotToAServer(server int, snapshot []byte) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludedIndex,
		rf.lastIncludedTerm,
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
	localIndex := rf.getLogLocalIndex(index)
	rf.lastIncludedTerm = rf.log[localIndex].Term
	rf.log = rf.log[localIndex:] // 保留最后一个 log entry
	data := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	rf.mu.Unlock()

	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendInstallSnapshotToAServer(i, snapshot)
	}
}

// service -> raft
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return false
	}
	return false
}
