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
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.sendRPC("Raft.InstallSnapshot", server, args, reply)
}

// service -> raft
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()

	rf.mu.Unlock()
}

// service -> raft
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		lastIncludedIndex,
		lastIncludedTerm,
		0,
		snapshot,
		true,
	}
	reply := InstallSnapshotReply{}
	for i := 0; i < rf.serverNum; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendInstallSnapshot(i, &args, &reply)
	}
	return false
}

func (rf *Raft) SaveStateAndSnapshot() {

}
