package raft

func (rf *Raft) apply() {
	for commitIndex := range rf.commitCh {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		DPrintf(4, "me: [%d], currentTerm [%d], lastApplied %v, get commitIndex %v\n", rf.me, rf.currentTerm, rf.lastApplied, commitIndex)
		if commitIndex > rf.lastApplied {
			if rf.lastApplied+1 < rf.log[0].Index {
				applyMsg := ApplyMsg{
					CommandValid: true,
					ReadSnapshot: true,
				}
				rf.lastApplied = rf.lastIncludedIndex // lastIncludedIndex 与 snapshot 是同步更新的
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
			} else {
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					applyMsg := ApplyMsg{
						true,
						rf.getLog(i).Command,
						i,
						false,
					}
					rf.lastApplied++
					rf.mu.Unlock()
					rf.applyCh <- applyMsg
					rf.mu.Lock()
					DPrintf(4, "me: [%d], currentTerm [%d], commit %v\n", rf.me, rf.currentTerm, applyMsg.Command)
				}
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) applyCommit(commitIndex int) {
	rf.commitCh <- commitIndex
}
