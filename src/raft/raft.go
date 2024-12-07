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

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "src/labrpc"

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	applyCh   chan ApplyMsg

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           int // 1: follower, 2: candidate 3: leader
	currentTerm     int
	votedFor        int
	electionTimeout time.Duration
	lastHeartbeat   time.Time
	electionBegin   time.Time
	voteCount       int

	log         []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == 3

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		log.Fatalln("error happen while encoding rf.currentTerm: ", err.Error())
	}
	if err := e.Encode(rf.votedFor); err != nil {
		log.Fatalln("error happen while encoding rf.votedFor: ", err.Error())
	}
	if err := e.Encode(rf.log); err != nil {
		log.Fatalln("error happen while encoding rf.log: ", err.Error())
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	if err := d.Decode(&rf.currentTerm); err != nil {
		log.Fatalln("error happen while decoding rf.currentTerm: ", err.Error())
	}
	if err := d.Decode(&rf.votedFor); err != nil {
		log.Fatalln("error happen while decoding rf.votedFor: ", err.Error())
	}
	if err := d.Decode(&rf.log); err != nil {
		log.Fatalln("error happen while decoding rf.log: ", err.Error())
	}

}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// term too old
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}
	// 如果是candidate或者旧term的leader，转变为follower
	if rf.state != 1 {
		//fmt.Println(time.Now(), "another leader ", args.LeaderId, " is exist...return to follower ", rf.me, " state ", rf.state)
		rf.transToFollower()
	}

	rf.mu.Lock()
	rf.currentTerm = args.Term
	rf.lastHeartbeat = time.Now()
	rf.persist()
	rf.mu.Unlock()
	//fmt.Println(time.Now(), " follower ", rf.me, " get heartbeat/append ", args)
	// 如果没有前置的entry，证明和新leader的log完全对不上
	//if args.PrevLogIndex > 0 {
	//	fmt.Println(time.Now(), " follower ", rf.me, " len of log ", len(rf.log), " prevlogindex ", args.PrevLogIndex, " prevlogterm ", args.PrevLogTerm, " rf.prevlogterm ", rf.log[args.PrevLogIndex-1].Term)
	//} else {
	//	fmt.Println(time.Now(), " follower ", rf.me, " len of log ", len(rf.log), " prevlogindex ", args.PrevLogIndex, " prevlogterm ", args.PrevLogTerm)
	//}
	//fmt.Println(time.Now(), " follower ", rf.me, " bool ", args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm)
	if args.PrevLogIndex >= 1 && args.PrevLogTerm >= 0 {
		if len(rf.log) < args.PrevLogIndex ||
			(args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
			//fmt.Println(time.Now(), " follower ", rf.me, " append false from ", args.LeaderId)
			reply.Success = false
			return
		}
	}

	rf.mu.Lock()
	// append entry
	if args.Entries != nil && len(args.Entries) > 0 {
		for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex+len(args.Entries); i++ {
			entry := args.Entries[i-args.PrevLogIndex-1]
			if len(rf.log) > i-1 {
				if rf.log[i-1].Term != entry.Term {
					rf.log = rf.log[:i-1]
					rf.log = append(rf.log, entry)
				}
			} else {
				rf.log = append(rf.log, entry)
			}
			//fmt.Println(time.Now(), " follower ", rf.me, " append ", i, " len of log ", len(rf.log))
		}
	}
	rf.persist()
	rf.mu.Unlock()

	//fmt.Println(time.Now(), " follower ", rf.me, " get heartbeat from leader ", args.LeaderId, " leadercommit ", args.LeaderCommit)
	if args.LeaderCommit > rf.commitIndex {
		oldCommitIndex := rf.commitIndex
		rf.mu.Lock()
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		//fmt.Println(time.Now(), " in Lock follower ", rf.me, " commit from ", oldCommitIndex+1, " to ", rf.commitIndex)
		rf.mu.Unlock()
		for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{
				Index:   i,
				Command: rf.log[i-1].Command,
			}
			rf.applyCh <- applyMsg
			//fmt.Println(time.Now(), " follower ", rf.me, " apply index ", i, " command ", rf.log[i-1].Command)
		}
	}

	reply.Success = true
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// 判断对方的log是否跟得上自己的
func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {
	index := 0
	term := -1
	if len(rf.log)-1 >= 0 {
		index = len(rf.log)
		term = rf.log[index-1].Term
	}
	if term != lastLogTerm {
		return term < lastLogTerm
	}
	return index <= lastLogIndex
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	// Your code here.

	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm &&
		rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {

		reply.VoteGranted = true
		rf.mu.Lock()
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.mu.Unlock()
		//fmt.Println(time.Now(), rf.me, " term ", rf.currentTerm, "vote ", reply.VoteGranted, " to ", args.CandidateId, args.Term)
	} else {
		//fmt.Println(time.Now(), rf.me, " term ", rf.currentTerm, "vote ", reply.VoteGranted, " to ", args.CandidateId,
		//	args.Term, args.Term >= rf.currentTerm,
		//	rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex),
		//	rf.votedFor == -1 || rf.votedFor == args.CandidateId)
		reply.VoteGranted = false
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Println("sendRequestVote ", server, args.Term, args.CandidateId)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) tryRequestVote(server int) {
	//fmt.Println(rf.me, " try get vote from ", server)
	reply := &RequestVoteReply{}
	peerReply := false
	peerNotReply := true
	for {
		isRequestVoteStop := !(rf.state == 2 && rf.electionBegin.Add(rf.electionTimeout).After(time.Now()))
		if isRequestVoteStop {
			return
		}
		if peerReply {
			// 获得选票
			if reply.VoteGranted {
				//fmt.Println("vote granted from ", server, " to ", rf.me)
				rf.mu.Lock()
				if rf.state == 2 {
					rf.voteCount++
				}
				rf.mu.Unlock()
				return
			}
			// 遇到Term严格大于自己的candidate，退回follower
			if !reply.VoteGranted && reply.Term > rf.currentTerm {
				rf.transToFollower()
				return
			}
			return
		}

		lastLogIndex := 0
		lastLogTerm := -1
		if len(rf.log)-1 >= 0 {
			lastLogIndex = len(rf.log)
			lastLogTerm = rf.log[len(rf.log)-1].Term
		}
		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply = &RequestVoteReply{}
		//fmt.Println("ready to send")

		chRequestVoteResult := make(chan bool)
		go func() {
			RequestVoteResult := rf.sendRequestVote(server, args, reply)
			chRequestVoteResult <- RequestVoteResult
		}()
		peerReply = false
		peerNotReply = false
		//var peerResp bool
		for peerReply == false && peerNotReply == false {
			select {
			case _ = <-chRequestVoteResult:
				peerReply = true
			case <-time.After(getRandMillsDuration(20, 30)):
				peerNotReply = true
			}
		}

		time.Sleep(getRandMillsDuration(10, 20))

		//fmt.Println("Vote Req ", ok)
	}
}

func getRandMillsDuration(left, right int) time.Duration {
	randMilliseconds := rand.Intn(right-left) + left
	return time.Duration(randMilliseconds) * time.Millisecond
}

func (rf *Raft) election() {
	rf.transToCandidate()

	//fmt.Println(rf.electionBegin, "start election ", rf.me, rf.currentTerm, rf.votedFor)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.tryRequestVote(i)
	}
	for {
		isElectionTimeout := rf.electionBegin.Add(rf.electionTimeout).Before(time.Now())
		if isElectionTimeout {
			break
		}

		isElectionSuc := rf.voteCount*2 > len(rf.peers) || ((rf.voteCount+1)*2 > len(rf.peers) && rf.votedFor == -1)
		if isElectionSuc {
			//fmt.Println(time.Now(), "election success", rf.me, rf.currentTerm, rf.votedFor)
			rf.transToLeader()
			return
		}
		// 其他原因导致退化为follower
		// 例如new leader产生，接收到AppendEntries，变回follower
		state := rf.state
		if state == 1 {
			time.Sleep(getRandMillsDuration(50, 250))

			//fmt.Println(time.Now(), "return to follower", rf.me, rf.currentTerm, rf.votedFor)
			return
		}
	}
	//fmt.Println(time.Now(), " election fail ", rf.me, rf.currentTerm, rf.votedFor)
	rf.transToFollower()
	time.Sleep(getRandMillsDuration(100, 500))
}

func (rf *Raft) transToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = 1
	rf.voteCount = 0
	rf.votedFor = -1
	rf.electionTimeout = getRandMillsDuration(100, 500)
	rf.persist()
	//fmt.Println(time.Now(), "trans to follower", rf.me, rf.currentTerm)
}

func (rf *Raft) transToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = 2
	rf.currentTerm += 1
	rf.voteCount = 0
	rf.votedFor = -1
	rf.electionTimeout = getRandMillsDuration(100, 500)
	rf.electionBegin = time.Now()
	rf.persist()
	//fmt.Println(time.Now(), "trans to candidate ", rf.me, rf.currentTerm)
}

func (rf *Raft) tryHeartbeat(server int) {
	//fmt.Println(rf.me, " try get vote from ", server)
	peerReply := false
	peerNotReply := true
	reply := &AppendEntriesReply{}
	args := AppendEntriesArgs{}
	for {
		state := rf.state
		if state != 3 {
			break
		}
		//fmt.Println(time.Now(), rf.me, rf.currentTerm, " send appendentries to ", server)
		// 如果有reply，需要根据reply.Success做一些更新
		if peerReply {
			if reply.Success {
				// append成功
				rf.mu.Lock()
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.mu.Unlock()
			} else {
				//fmt.Println(time.Now(), " leader ", rf.me, " append false ", args)
				// rejoin之后term低于原来的follower，说明已经有新的leader term了，退化为follower
				if reply.Term > rf.currentTerm {
					//fmt.Println(time.Now(), " leader ", rf.me, " turn to follower because there is newer follower ")
					rf.transToFollower()
					break
				}
				if rf.nextIndex[server] >= 2 {
					rf.mu.Lock()
					rf.nextIndex[server]--
					//fmt.Println(time.Now(), " leader ", rf.me, " subtract nextIndex ", rf.nextIndex[server])
					rf.mu.Unlock()
				}
			}
		}
		prevLogIndex := 0
		prevLogTerm := -1
		if rf.nextIndex[server]-2 >= 0 {
			prevLogIndex = rf.nextIndex[server] - 1
			prevLogTerm = rf.log[rf.nextIndex[server]-2].Term
		}
		var entries []Entry
		if len(rf.log) >= rf.nextIndex[server] {
			entries = rf.log[rf.nextIndex[server]-1 : len(rf.log)]
		}
		args = AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply = &AppendEntriesReply{}

		chAppendEntriesResult := make(chan bool)
		go func() {
			appendEntriesResult := rf.sendAppendEntries(server, args, reply)
			chAppendEntriesResult <- appendEntriesResult
		}()
		peerReply = false
		peerNotReply = false
		//var peerResp bool
		for peerReply == false && peerNotReply == false {
			select {
			case _ = <-chAppendEntriesResult:
				peerReply = true
			case <-time.After(getRandMillsDuration(20, 30)):
				peerNotReply = true
			}
		}
		time.Sleep(getRandMillsDuration(10, 20))
		//fmt.Println(time.Now(), " tryAppendEntries ", ok)
	}
}

func (rf *Raft) heartbeat() {
	//fmt.Println(time.Now(), " heartbeat start server: ", rf.me, " term: ", rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.tryHeartbeat(i)
	}
}

func (rf *Raft) transToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = 3

	rf.nextIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers), len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) + 1
		rf.matchIndex[i] = 0
	}

	go rf.heartbeat()
	//fmt.Println(time.Now(), "trans to leader ", rf.me, rf.currentTerm)

}

func (rf *Raft) electionTrigger() {
	//fmt.Println("election trigger ", rf.me)
	for {
		//time.Sleep(getRandMillsDuration(300, 800))
		toElection := rf.state == 1 && rf.lastHeartbeat.Add(rf.electionTimeout).Before(time.Now())
		if toElection {
			rf.election()
		}
		time.Sleep(getRandMillsDuration(10, 20))
	}
}

func (rf *Raft) tryAgreement(entry Entry) {
	// 如果超过半数加入,那么commit
	for {
		state := rf.state
		if state != 3 {
			break
		}
		if entry.Index <= rf.commitIndex {
			//fmt.Println(time.Now(), " leader ", rf.me, " index ", entry.Index, " is passively committed ")
			break
		}
		count := 0
		for _, matchIndex := range rf.matchIndex {
			//fmt.Println(time.Now(), " leader ", rf.me, " peer ", i, " matchIndex ", matchIndex, " index to commit ", entry.Index)
			if matchIndex >= entry.Index {
				count++
			}
		}
		//fmt.Println(time.Now(), " leader ", rf.me, " check match count for index ", entry.Index, " is ", count)

		if count*2 >= len(rf.peers) {
			oldCommitIndex := rf.commitIndex
			rf.mu.Lock()
			rf.commitIndex = max(rf.commitIndex, entry.Index)
			//fmt.Println(time.Now(), " leader ", rf.me, " commit from ", oldCommitIndex+1, " to ", rf.commitIndex)
			rf.mu.Unlock()
			// apply新commit的log entry
			for i := oldCommitIndex + 1; i <= rf.commitIndex; i++ {
				applyMsg := ApplyMsg{
					Index:   i,
					Command: rf.log[i-1].Command,
				}
				rf.applyCh <- applyMsg
				//fmt.Println(time.Now(), " leader ", rf.me, " apply index ", i, " command ", rf.log[i-1].Command)
			}

			break
		}
		time.Sleep(getRandMillsDuration(10, 20))
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := rf.state == 3
	if !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	index = len(rf.log) + 1
	term = rf.currentTerm
	entry := Entry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.nextIndex[rf.me]++
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me] - 1
	rf.persist()
	rf.mu.Unlock()
	//fmt.Println(time.Now(), " leader ", rf.me, " start command ", command, " index ", index)
	// start agreement
	go rf.tryAgreement(entry)

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	//fmt.Println("A new raft instance: ", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here.
	rf.state = 1
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionTimeout = getRandMillsDuration(100, 500)
	rf.lastHeartbeat = time.Now()
	rf.voteCount = 0

	rf.log = make([]Entry, 0, 10)
	rf.commitIndex = 0
	rf.lastApplied = 0

	go rf.electionTrigger()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
