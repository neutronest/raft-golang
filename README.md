# raft-golang

# Introduction
An experimental projects that implement a simple raft distributed protocol which motivated by MIT 6.824 course. 

# Used

cd raft && go test -run 2A/2B/2C | grep -E "Test|Pass|Fail"

then observe the running result.

# Plan

Basic Implement
+ [x] Leader Election
+ [x] Log Replica
+ [x] Basic Persist 

Optimized Variants 
+ [x] Batch version of append entry rpc
+ [x] rollback multistep when prevLogTerm-prevLogIndex is not match 
+ [ ] fix data race problem 

Advanced
+ [ ] Snapshot features supported  
+ [ ] Implement a kv storage based on this raft framework
+ [ ] others...

Others
