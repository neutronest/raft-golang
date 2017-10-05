package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

import "time"
import "math/rand"
import "fmt"
import "bytes"
import "encoding/gob"


// import "bytes"
// import "encoding/gob"

// CUSTOM Settings
const (
    STATE_FOLLOWER = 0
    STATE_CANDIDATE = 1
    STATE_LEADER = 2
)


const TICK_TIME = 50
const APPENDTYPE_HEARTBEAT = 0
const APPENDTYPE_COMMAND = 1

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {

	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

func NewApplyMsg (index int, command interface{}, ifUseSnapshot bool, Snapshot []byte) *ApplyMsg {

    applyMsg := &ApplyMsg{}
    applyMsg.Index = index
    applyMsg.Command = command
    applyMsg.UseSnapshot = ifUseSnapshot
    applyMsg.Snapshot = Snapshot
    return applyMsg
}

type LogEntry struct {

    Term int
    Index int
    Command interface{}
}

func NewLogEntry(term int, index int, command interface{}) *LogEntry {
    
    logEntry := &LogEntry{}
    logEntry.Term = term
    logEntry.Index = index
    logEntry.Command = command
    return logEntry
}

type LeaderElect struct {

    // for follower or candidate: if this follower has been voted for somebody yet
    // 
    HasVoted bool

    // statistic the number of server
    // which agree the candidate to be leader
    VotedGrantNum int

    // used for leader
    // the connect aviable state for each server
    PeerStates []int
}

type LogReplica struct {

    // the elements of Log Replication

    // local log storage
    // store all history command entries
    Log []*LogEntry

    // the commited highest index
    CommitIndex int

    // the applied highest index
    LastAppliedIndex int

    // only for leader
    // the next entry index tobe applied for each server
    NextIndex []int

    // only for leader
    // the highest index of replicated entries in each followers
    MatchIndex []int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// follower , candidate or leader ?
	Identity int

	// the timestamp of whole system
	Term int

	// the channel used to
    // receive each log-entry or just heartbeats
    // for each server
    // leader : used to receive client request command
    // follower : used to receive leader's replicated log
	AppendEntryChan chan LogEntry

    ApplyMsgChan chan ApplyMsg

    ToLeaderChan chan bool

    RecycleChan chan bool

    HeartBeatChan chan bool

    LeaderElect *LeaderElect

    LogReplica *LogReplica
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
    //fmt.Printf("rf %d's status:%d  term:%d \n", rf.me, rf.Identity, rf.Term)
	term = rf.Term
	if rf.Identity == STATE_LEADER {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
    // rf.persister.SaveRaftState(data)
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    e.Encode(rf.Term)
    e.Encode(rf.LeaderElect.HasVoted)
    e.Encode(rf.LogReplica.Log)
    data := w.Bytes()
    rf.persister.SaveRaftState(data)
    

}

//
// restore previously persisted state.

//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
    }
    
    r := bytes.NewBuffer(data)
    d := gob.NewDecoder(r)
    d.Decode(&rf.Term)
    d.Decode(&rf.LeaderElect.HasVoted)
    d.Decode(&rf.LogReplica.Log)
    return
}

// request vote
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	RequestIdentity int

	// the candidate's term
	RequestTerm int

	// candidate
	CandidateId int

    //
    LastLogTerm int

    //
    LastLogIndex int

	// TODO
	// log entry

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).

	// the updated (if nessarery) Term
	UpdateTerm int

	// grant the candidate's vote or not?
	VoteGranted bool

    //
    IdentityDeny bool

    //
    ServerBrokenDeny bool

}



//
// example RequestVote RPC handler.
// the "rf *Raft" is the target server that agree or reject the request
// the args is just the info of origin server which want to become leader
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
    

    go func(){
        rf.RecycleChan <- true
    }()

    guestTerm := args.RequestTerm
	//guestId := args.CandidateId
	guestIdentity := args.RequestIdentity

    // check request if legal
    // check Term
    // check Identity
    // check rf's Identity

    
    lastEntryIndex := 0
    lastEntryTerm := 1
    lenOfLog := len(rf.LogReplica.Log)
    if lenOfLog != 0 {
        lastEntry := rf.LogReplica.Log[lenOfLog-1]
        lastEntryTerm = lastEntry.Term
        lastEntryIndex = lastEntry.Index

    }


    if rf.Identity == STATE_LEADER {
        DebugElect(DEBUG_FLAG, "fuck go back to your demmy follower!\n")
        reply.UpdateTerm = rf.Term
        reply.ServerBrokenDeny = true
    }
    
    // leader restrict
    // check if the follower is out-of-date than candidate
    // which is more up-to-date ? 
    //DebugElect(DEBUG_FLAG, "candi %d request vote of %d \n", args.CandidateId, rf.me)
    if lastEntryTerm > args.LastLogTerm {
        reply.IdentityDeny = true
    } else if lastEntryTerm == args.LastLogTerm {
        if lastEntryIndex > args.LastLogIndex {
            reply.IdentityDeny = true
        } 
    }

    if reply.IdentityDeny {
        DebugElect(DEBUG_FLAG, "cadi %d is out-of-date checked by %d\n", args.CandidateId, rf.me)
        if rf.Term < guestTerm {
            rf.Term = guestTerm
        }
    }

	// check if term legal
	if rf.Term > guestTerm {
		reply.UpdateTerm = guestTerm
		reply.VoteGranted = false
		return
	} else if rf.Term < guestTerm {
        
        if reply.IdentityDeny == false {
            rf.Term = guestTerm
            rf.ToFollower()
        }
	}

	// check if identity legal
	if guestIdentity != STATE_CANDIDATE || rf.Identity != STATE_FOLLOWER {
		reply.VoteGranted = false
		return
	}


	// check if rf voted before
	if rf.LeaderElect.HasVoted  {
		reply.VoteGranted = false
        return
	} else if reply.IdentityDeny == false {
        reply.VoteGranted = true
        rf.LeaderElect.HasVoted = true

    }

    // trigger for timer reset
    return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    
    rf.persist()
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


type AppendEntryArgs struct {


    // 1: normal entry
    // 0: heartbeats
    AppendType int

    // The currnet term of Leader
    Term int
    
    // the leader id
    LeaderId int

    // the previous logentry's term of leader
    PrevLogTerm int

    // the previous logentry's index of leader
    PrevLogIndex int

    //// batch of entries that to be appended
    Entry LogEntry

    // the entry index which has been committed by leader
    LeaderCommitIndex int
	// TODO

}

type AppendEntryReply struct {

	// the updated (if ..) Term
	UpdateTerm int

    // the log replica status during append entry
    // 0: replicate success
    // 1: follower's log is shorter then leader
    // 2: follower's log is larger than leader
    // 3: follower's log has the same length but diff with leader
    AppendStatus int
    
	//
	Result bool

	//TODO

}



// AppendEntry RPC Handler
// the follower (rf) receive one entry from leader
// and decide if accept repicate it or not
// the "rf *Raft" is the target that apllied the entry
// the args is the info of leader
// === POLICY:
// --- about Term
// if leader's Term < follower's Term then refuse the request and return follow's Term
// if leader's Term > follower's Term, just broadcast the leader's Term to follower
// --- about log replica
// if leader's lastPrevTerm and lastPrevIndex match the lastest entry's term and log then replicate the entry to it's local log storage and return true
// if not, return false
// --- about log commit
// the request must contain a leader commitIndex
// which means current safety commited index by leader
// if the follower's commitIndex < leader's lastCommit, then update the follower's commitIndex
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
    
        if rf.Identity != STATE_FOLLOWER {
            
            reply.Result = false
            return
        }

        // check term correctness
        if rf.Term > args.Term {
            // reject
            fmt.Printf("RPC-ApplyEntry rej: follower%d-term: %d, leader%d-term: %d\n", rf.me, rf.Term, args.LeaderId, args.Term)
            reply.UpdateTerm = rf.Term
            reply.Result = false
            return
        }
        // if follower's term < leader's term
        // just increase 
        if rf.Term < args.Term {
            rf.Term = args.Term
        }

        // check consistency
        lastLeaderTerm := args.PrevLogTerm
        lastLeaderIndex := args.PrevLogIndex
        lenOfLog := len(rf.LogReplica.Log)

        lastLeaderLogPos := args.PrevLogIndex-1
        newLeaderLogPos := lastLeaderLogPos + 1

        // check if contain an entry at prevLogTerm-prevLogIndex
        reply.AppendStatus = 0
        if lastLeaderLogPos > 0 && lenOfLog > lastLeaderLogPos {
            lastEntry := rf.LogReplica.Log[lastLeaderLogPos]
            if lastEntry.Term != lastLeaderTerm || lastEntry.Index != lastLeaderIndex {
                // delete all after entries
                DebugReplica(DEBUG_FLAG, "*** node %d delete all entries after index:%d\n", rf.me, lastLeaderLogPos+1)
                rf.LogReplica.Log = rf.LogReplica.Log[:(lastLeaderLogPos)]
                GetLogInfo(rf)
                reply.AppendStatus = 1
            } else if lenOfLog > newLeaderLogPos {
                // if the follower has got entry at same index, check if consist
                curEntry := rf.LogReplica.Log[newLeaderLogPos]
                if curEntry.Term != args.Entry.Term || curEntry.Index != args.Entry.Index {
                    // delete all after entries
                    DebugReplica(DEBUG_FLAG, "&&& node %d delete all entries after index:%d\n", rf.me, lastLeaderIndex+1)
                    rf.LogReplica.Log = rf.LogReplica.Log[:newLeaderLogPos]
                    GetLogInfo(rf)
                }
            }


        } else if lenOfLog < lastLeaderIndex {
            // not enough entry
            reply.AppendStatus = 1
            DebugReplica(DEBUG_FLAG, "*** node %d entry len is not enough\n", rf.me)
            GetLogInfo(rf)
        }
    

        if reply.AppendStatus == 0 && args.AppendType == 1 {
            
            newEntry := NewLogEntry(args.Entry.Term, args.Entry.Index, args.Entry.Command)
            rf.LogReplica.Log = append(rf.LogReplica.Log, newEntry)
        }

        go func() {
            rf.HeartBeatChan <- true 
        }()

        if reply.AppendStatus != 0 {
            return
        }

        // if follower's commitIndex < leader's latest commitIndex
        // and /
        if rf.LogReplica.CommitIndex < args.LeaderCommitIndex && len(rf.LogReplica.Log) >= args.LeaderCommitIndex {
            

            DebugReplica(DEBUG_FLAG, "fow %d commit by args info: %+v\n",rf.me, args)
            rf.LogReplica.CommitIndex += 1
            DebugReplica(DEBUG_FLAG, "follower %d: commitIndex:%d\n", rf.me, rf.LogReplica.CommitIndex)
            entry := rf.LogReplica.Log[rf.LogReplica.CommitIndex-1]
            msg := NewApplyMsg(entry.Index, entry.Command, false, nil)
            go func() {
                rf.ApplyMsgChan <- *msg
            }()
        }
        // trigger: update the server connect
        reply.Result = true
        return
    }
    
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {

    rf.persist()
    ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
    return ok
}












//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    
    //index := -1


    rf.mu.Lock()
    defer rf.mu.Unlock()
	// Your code here (2B).
    // TODO 2B
    if rf.Identity != STATE_LEADER {
        //DebugReplica(DEBUG_FLAG, "fail to exeu command %v, %d is not leader now \n", command, rf.me)
        return -1, -1, false
    }

    term := rf.Term
    
    curIndex := len(rf.LogReplica.Log)
    entry := NewLogEntry(term, curIndex+1, command)
    rf.LogReplica.Log = append(rf.LogReplica.Log, entry)
    rf.LogReplica.MatchIndex[rf.me] = len(rf.LogReplica.Log)
    GetLogInfo(rf)
	return curIndex+1, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	
	// Your initialization code here (2A, 2B, 2C).
    rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
    rf.AppendEntryChan = make(chan LogEntry, 1)
    rf.ToLeaderChan = make(chan bool, 1)
    rf.RecycleChan = make(chan bool, 1)
    rf.HeartBeatChan = make(chan bool, 1)
    rf.ApplyMsgChan = applyCh

    LeaderElect := &LeaderElect{}
    LeaderElect.HasVoted = false
    LeaderElect.VotedGrantNum = 0
    LeaderElect.PeerStates = make([]int, len(rf.peers))
    rf.LeaderElect = LeaderElect

    logReplica := &LogReplica{}
    logReplica.Log = make([]*LogEntry, 0)
    logReplica.CommitIndex = 0
    logReplica.LastAppliedIndex = 0
    logReplica.NextIndex = make([]int, len(rf.peers), len(rf.peers))

    logReplica.MatchIndex = make([]int, len(rf.peers), len(rf.peers))
    rf.LogReplica = logReplica
    

	// Timter init
	// Identity init
	rf.ToFollower()
	// Term init
	rf.Term = 0
    go rf.StateMachineDriver()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// ========= Timer recycle ==================

func GenElectTimeout() int {

	timeInterval := rand.Intn(450) + 450
	return timeInterval

}

func (rf *Raft) ToFollower() {

	rf.Identity = STATE_FOLLOWER
	rf.LeaderElect.VotedGrantNum = 0
	rf.LeaderElect.HasVoted = false
}

func (rf *Raft) ToCandidate() {

	rf.Identity = STATE_CANDIDATE
	rf.LeaderElect.VotedGrantNum = 1
    rf.LeaderElect.HasVoted = true
    rf.Term += 1
}

func (rf *Raft) ToLeader() {

    rf.Term = rf.Term + 1    
	rf.Identity = STATE_LEADER
	rf.LeaderElect.VotedGrantNum = 0
	rf.LeaderElect.HasVoted = false
    rf.LogReplica.NextIndex = make([]int, len(rf.peers))
    LenOfLog := len(rf.LogReplica.Log)
    for idx :=0; idx < len(rf.peers); idx++ {
        rf.LogReplica.NextIndex[idx] = LenOfLog+1
    }
    GetLogInfo(rf)
}


func (rf *Raft) CheckState(t int) {

    ticker := time.NewTicker(10 * time.Millisecond) // check for every 10ms
	for _ = range ticker.C {
        state := ""
        if rf.Identity == STATE_FOLLOWER {
            state = "Follower"
        } else if rf.Identity == STATE_CANDIDATE {
            state = "Candidate"
        } else {
            state = "Leader"
        }

        fmt.Printf("%d: server %d's state is %s \n", t, rf.me, state)
    }
}


func (rf *Raft) BoardcastRequestVote() {
    // send request vote to other nodes
    
    for idx := 0; idx < len(rf.peers); idx++ {
        go func(idx_val int) {
            if idx_val == rf.me {
                rf.LeaderElect.PeerStates[idx_val] = 1
                return
            }
            DebugElect(DEBUG_FLAG, "%d set request vote to %d\n", rf.me, idx_val)
            lenOfLog := len(rf.LogReplica.Log)

            voteArgs := RequestVoteArgs{}
            voteArgs.RequestIdentity = rf.Identity
            voteArgs.RequestTerm = rf.Term
            voteArgs.CandidateId = rf.me
            if lenOfLog != 0 {
                lastEntry := rf.LogReplica.Log[lenOfLog-1]
                voteArgs.LastLogTerm = lastEntry.Term
                voteArgs.LastLogIndex = lastEntry.Index
            } else {
                voteArgs.LastLogTerm = 1
                voteArgs.LastLogIndex = 0
            }

            voteReply := RequestVoteReply{}
            voteReply.IdentityDeny = false
            voteReply.ServerBrokenDeny = false
            ok := rf.sendRequestVote(idx_val, &voteArgs, &voteReply)
            if !ok {
                //DebugElect(DEBUG_FLAG, "%d has been disconnected \n", idx_val)
                rf.LeaderElect.PeerStates[idx_val] = 0
            } else {
                rf.LeaderElect.PeerStates[idx_val] = 1
                //if voteReply.
                if voteReply.VoteGranted {
                    rf.LeaderElect.VotedGrantNum = rf.LeaderElect.VotedGrantNum + 1
                    go func(){

                        rf.RecycleChan <- true
                    }()
                } else {
                    if voteReply.UpdateTerm > rf.Term {
                        // rf is out of date
                        rf.Term = voteReply.UpdateTerm
                        rf.ToFollower()
                    }

                    if voteReply.ServerBrokenDeny {
                        rf.Term = voteReply.UpdateTerm
                        DebugElect(DEBUG_FLAG, "%d got serverbroken denied!\n", rf.me)
                        rf.ToFollower()
                    }
                    if voteReply.IdentityDeny {
                        DebugElect(DEBUG_FLAG, "%d got identity denied by %d\n", rf.me, idx_val)
                        rf.ToFollower()
                    }
                } // end else...
            } // end for send requestvote
        }(idx)
    } // endfor send request vote

    rf.CheckbeLeader()
}

func (rf *Raft) CheckbeLeader() {
    // calculate the vote num
    DebugElect(DEBUG_FLAG, "%d's elect state: %d / %d \n",rf.me,  rf.LeaderElect.VotedGrantNum, len(rf.peers))
    if float32(rf.LeaderElect.VotedGrantNum) / float32(len(rf.peers))  > 0.5 {
        // has gain the major numer
        // to be the leader!!!
        go func() {
            rf.ToLeaderChan <- true
        }()
    }   
}

func (rf *Raft) BoardcastAppendEntry() {
    //DebugReplica(DEBUG_FLAG, "%d forkIO a appendEntry\n", rf.me)
    if rf.Identity != STATE_LEADER {
        return
    }

    lenOfLog := len(rf.LogReplica.Log)
    for idx :=0; idx < len(rf.peers); idx++ {
        go func(idx_val int) {
            //defer DebugReplica(DEBUG_FLAG, "%d -> %d append rpc finished\n", rf.me, idx_val)
            if idx_val == rf.me {
                rf.LeaderElect.PeerStates[idx_val] = 1
                return
            }
            if rf.Identity != STATE_LEADER {
                return
            }

            nextIndex := rf.LogReplica.NextIndex[idx_val]
            appendArgs := AppendEntryArgs{}
            appendReply := AppendEntryReply{}

            // default init
            appendArgs.Term = rf.Term
            appendArgs.LeaderId = rf.me
            appendArgs.PrevLogTerm = rf.Term
            appendArgs.PrevLogIndex = 0
            appendArgs.AppendType = 1
            appendArgs.LeaderCommitIndex = rf.LogReplica.CommitIndex
            if nextIndex-2 >= 0 {
                // lastEntry is not empty
                lastEntry :=  rf.LogReplica.Log[nextIndex-2]
                appendArgs.PrevLogIndex = lastEntry.Index
                appendArgs.PrevLogTerm = lastEntry.Term
            }

            if nextIndex-1 < len(rf.LogReplica.Log) && lenOfLog != 0 {
                // need to send entry to follower
                
            
                entry := LogEntry(*rf.LogReplica.Log[nextIndex-1])
                appendArgs.Entry = *(NewLogEntry(entry.Term, entry.Index, entry.Command))
                //DebugReplica(DEBUG_FLAG, "%d -> %d, send entry %d-%d/%d \n", rf.me, idx_val, entry.Term, entry.Index, entry.Command.(int))
            } else {
                // send heartbeats
                appendArgs.AppendType = 0
                appendArgs.Entry = LogEntry{}
                appendArgs.Entry.Command = nil
            }                

            ok := rf.sendAppendEntry(idx_val, &appendArgs, &appendReply)
            if !ok {
                // the sever has been disconnected
                if rf.LeaderElect.PeerStates[idx_val] != 0 {
                    DebugElect(DEBUG_FLAG, "leader %d report: server %d is broken!\n", rf.me, idx_val)
                }
                rf.LeaderElect.PeerStates[idx_val] = 0
                return
            }
            rf.LeaderElect.PeerStates[idx_val] = 1
            if appendReply.Result == false {
                if rf.Term < appendReply.UpdateTerm  {
                    // oh! the leader is out of date
                    rf.Term = appendReply.UpdateTerm
                    DebugElect(DEBUG_FLAG, "OHOHOH leader %d out of date back to follower!\n", rf.me)
                    rf.ToFollower()
                    
                }
                if appendReply.AppendStatus == 1 {
                    DebugReplica(DEBUG_FLAG, "get diff from %d at index %d\n", idx_val, appendArgs.Entry.Index)
                    rf.LogReplica.NextIndex[idx_val] -= 1
                }
            } else {
                // success append entry
                
                if appendArgs.AppendType == 1 {
                    DebugReplica(DEBUG_FLAG, "%d send entry to %d success\n", rf.me, idx_val)
                    rf.LogReplica.NextIndex[idx_val] += 1
                    rf.LogReplica.MatchIndex[idx_val] = rf.LogReplica.NextIndex[idx_val]-1
                }
            }// end applyResult
        }(idx)
    } // end for
    //DebugReplica(DEBUG_FLAG, "%d end a appendEntry\n", rf.me)


}


func (rf *Raft) StateMachineDriver() {
    
    fmt.Printf("begin server %d's driver\n", rf.me)
    for { 

        if rf.Identity == STATE_FOLLOWER {
            // TODO apply the commit entry
            select {
            case entry :=<- rf.AppendEntryChan:
                // TODO append log
                // Trick: check consistency in applyEntry RPC
                // update follower's commit index in applyEntry RPC
                if entry.Command != nil {
                    DebugReplica(DEBUG_FLAG, "node %d get entry to append\n", rf.me)
                    logEntry := NewLogEntry(entry.Term, entry.Index, entry.Command)
                    rf.LogReplica.Log = append(rf.LogReplica.Log, logEntry)
                } 
                rf.ToFollower()
            case <- rf.RecycleChan:
                continue
            case <- rf.HeartBeatChan:
                rf.ToFollower()
                continue
            case <- time.After(time.Duration(rand.Int63()%500 + 500)* time.Millisecond):
                DebugElect(DEBUG_FLAG, "no leader.. node %d become candi\n", rf.me)
                rf.ToCandidate()
            } // end select
            // END FOLLOWER
        } else if rf.Identity == STATE_CANDIDATE {

            rf.BoardcastRequestVote()
            
            select {
            case entry :=<- rf.AppendEntryChan:
                // the system has a leader yet!
                // change the identity back to follower
                if entry.Command != nil {
                    DebugReplica(DEBUG_FLAG, "node %d get entry to append\n", rf.me)
                    logEntry := NewLogEntry(entry.Term, entry.Index, entry.Command)
                    rf.LogReplica.Log = append(rf.LogReplica.Log, logEntry)
                } 
                rf.ToFollower()
            case <- rf.ToLeaderChan:
                DebugElect(DEBUG_FLAG, "candidate %d become leader!\n", rf.me)
                rf.ToLeader()
            case <- rf.RecycleChan:
                continue
            case <- rf.HeartBeatChan:
                continue
            case <- time.After(time.Duration(rand.Int63()%500 + 500)* time.Millisecond):
                rf.ToCandidate()    
            }
            // END CANDIDATE
        } else if rf.Identity == STATE_LEADER {


            rf.LeaderElect.PeerStates[rf.me] = 1

            // TODO check if applyMsgChan has apply msgs
            // bad design: this precedure must be applied by Start RPC
            
      
            //DebugElect(DEBUG_FLAG, "leader: %d\n", rf.me)
            rf.BoardcastAppendEntry()
            
            // check if self broken
            //DebugElect(DEBUG_FLAG, "leader %d's peer state: %v\n",rf.me, rf.LeaderElect.PeerStates)
            activeNum := 0
            allNum := len(rf.peers)
            for idx2:=0; idx2 < allNum; idx2++ {
                if rf.LeaderElect.PeerStates[idx2] == 1 {
                    activeNum += 1
                } else {
                    //DebugElect(DEBUG_FLAG, "leader %d confirm node %d is broken\n", rf.me, idx2)
                }
            }
            if activeNum == 1 {
                DebugElect(DEBUG_FLAG, "Ouch! the leader %d is broken itself!\n", rf.me)
                rf.ToFollower()
                continue
            }

            // update leader's commitIndex
            // get all active nodes' number
            if rf.LogReplica.CommitIndex < len(rf.LogReplica.Log) {
                laterCommitNum := 0
                for idx := 0; idx < allNum; idx++ {
                    if rf.LeaderElect.PeerStates[idx] == 1 {
                        if rf.LogReplica.CommitIndex < rf.LogReplica.MatchIndex[idx] {
                            laterCommitNum += 1
                        }
                    }
                }
                
                if float32(laterCommitNum) / float32(len(rf.peers)) > 0.5 {
                    
                    rf.LogReplica.CommitIndex += 1
                    entry := rf.LogReplica.Log[rf.LogReplica.CommitIndex-1]
                    msg := NewApplyMsg(entry.Index, entry.Command, false, nil)
                    go func() {
                        rf.ApplyMsgChan <- *msg
                    }()    
                }
            }
            time.Sleep(TICK_TIME*time.Millisecond)
        } // end LEADER
    }
}

// ========================================
