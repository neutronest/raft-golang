package raft

import "fmt"

const DEBUG_FLAG = ALL
const NONE = -1
const ELECT = 0
const REPLICA = 1
const ALL = 90


func DebugElect(flag int, msg string, args ...interface{}) {
    if flag == ELECT || flag == ALL {
        fmt.Printf(msg, args...)
    }
}


func DebugReplica(flag int, msg string, args ...interface{}) {
    if flag == REPLICA || flag == ALL {
        fmt.Printf(msg, args...)
    }
}

func GetLogInfo(rf *Raft) {

    log := rf.LogReplica.Log
    lenOfLog := len(log)
    fmt.Printf("%d's log: [ ", rf.me)
    for i := 0; i < lenOfLog; i++ {
        entry := rf.LogReplica.Log[i]
        fmt.Printf("%d-%d/%d ", entry.Term, entry.Index, entry.Command.(int))
    }
    fmt.Printf("]\n ")

}
