package raft

func (rf *Raft) apply() {
	for commitIndex := range rf.commitCh {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		DPrintf(2, "me: [%d], currentTerm [%d], get commitIndex %v\n", rf.me, rf.currentTerm, commitIndex)
		for i := rf.lastApplied + 1; i <= commitIndex; i++ {
			applyMsg := ApplyMsg{
				true,
				rf.log[i].Command,
				i,
			}
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			//DPrintf(2, "me: [%d], currentTerm [%d], commit %v\n", rf.me, rf.currentTerm, applyMsg.Command)
			rf.mu.Lock()
			rf.lastApplied++
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applyCommit(commitIndex int) {
	rf.commitCh <- commitIndex
}
