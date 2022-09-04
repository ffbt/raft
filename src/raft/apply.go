package raft

// applyCh 是由 commitCh 同步的
// CommandIndex 是递增的
func (rf *Raft) apply() {
	for commitIndex := range rf.commitCh {
		if rf.killed() {
			return
		}

		rf.mu.Lock()
		DPrintf(5, "me: [%d], currentTerm [%d], lastApplied %v, rf.log[0].Index %v, get commitIndex %v\n", rf.me, rf.currentTerm, rf.lastApplied, rf.log[0].Index, commitIndex)
		if commitIndex > rf.lastApplied {
			if rf.lastApplied+1 < rf.log[0].Index {
				// 存在没有 apply 的 log
				applyMsg := ApplyMsg{
					CommandValid: true,
					ReadSnapshot: true,
				}
				// 非常重要
				rf.lastApplied = rf.lastIncludedIndex // lastIncludedIndex 与 snapshot 是同步更新的
				rf.mu.Unlock()

				rf.applyCh <- applyMsg
			} else {
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					if rf.getLog(i).Command == nil {
						continue
					}

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
					DPrintf(5, "me: [%d], currentTerm [%d], commit [%v]%v\n", rf.me, rf.currentTerm, i, applyMsg.Command)
				}
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applyCommit(commitIndex int) {
	// 序列化
	rf.commitCh <- commitIndex
}
