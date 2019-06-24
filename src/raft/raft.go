package raft

//copyright by 湖南大学-邵靳天  sjt@hnu.edu.cn
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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)
import "6.824/src/labrpc"

// import "bytes"
// import "encoding/gob"

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER

	HEART_INTERVAL = 100
	MIN_ELECTION_INTERVAL = 400
	MAX_ELECTION_INTERVAL = 800

)
var stateDic = []string{"FOLLOWER","CANDIDATE","LEADER"}


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
	CommandValid	bool
	CommandIndex	int
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
	votedFor		int
	voteAcquired	int
	currentTerm		int32
	state   		int32

	electionTimer	*time.Timer//400~800ms

	appendCh		chan struct{}//log 通知载体
	voteCh			chan struct{}//成功投票的vote 通知载体

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.


}
//###确认状态
func(rf *Raft) isState(state int32) bool{


	ok := state == atomic.LoadInt32(&rf.state)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(rf.currentTerm)
	isleader = rf.isState(LEADER)

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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 		int32
	CandidateID int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int32
	VoteOrNot	bool
}

//
// example RequestVote RPC handler.
//
//此函数代表所有的节点收到candidate的 request 后做出的反应
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//这个函数也是原子性的，前面已经在election()函数中锁过了
	// if candidate正常广播投票
	rf.mu.Lock()
	rf.mu.Unlock()
	if args.Term == rf.currentTerm{
		// 某其余节点还未给别人投票,那就给这个candidate vote
		if rf.votedFor == -1{
			reply.VoteOrNot = true
			//更新信息，已投票
			rf.votedFor = args.CandidateID
		//已经给别人投了
		}else {
			reply.VoteOrNot = false
		}
	//candidate节点早进入下一阶段,那就强行将此节点转回follower，继续投票
	}else  if args.Term > rf.currentTerm{
		//不管怎么样退回follower
		rf.uptoState(FOLLOWER)
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		reply.VoteOrNot = true
	//candidate 过期了！！！
	}else if args.Term < rf.currentTerm{
		reply.VoteOrNot =false
		//后续在broadcastRV 函数中candidate转回follower
	}

	reply.Term = rf.currentTerm

	if reply.VoteOrNot == true{
		//投票成功消息写入channel of vote
		go func() {rf.voteCh <- struct{}{}}()
	}

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
//**********************
//仿照上面写心跳机制的RPC
func (rf *Raft) sendAppendEntry(server int ,args *AppendEntryArgs,reply *AppendEntryReply)bool{
	ok := rf.peers[server].Call("Raft.AppendEntry",args,reply)
	return ok

}
type AppendEntryArgs struct {
	Term 		int32
	LeaderID 	int
}
type AppendEntryReply struct {
	Term 		int32
	AppendOrNot	bool
}
//其他节点收到Append后的反应,rf指的是follower节点
func (rf *Raft) AppendEntry(args *AppendEntryArgs,reply *AppendEntryReply){
	//lock to block
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term == rf.currentTerm{
		reply.AppendOrNot = true
		//leader过期，回退到follower
	}else if(args.Term < rf.currentTerm){
		args.Term = rf.currentTerm
		reply.AppendOrNot = false
	}else {
		//follower过期
		rf.uptoState(FOLLOWER)
		rf.currentTerm = args.Term
		reply.AppendOrNot = true

	}
	reply.Term = rf.currentTerm
	if reply.AppendOrNot {
		go func() {
			rf.appendCh<- struct{}{}
		}()
	}


}
//***********************


func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
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
	//新的一个节点
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state= FOLLOWER
	rf.electionTimer = time.NewTimer(randElectionInterval())
	rf.votedFor = -1
	rf.appendCh = make(chan struct{})//no buffer
	rf.voteCh = make(chan  struct{})
	rf.voteAcquired =0;//获得0 votes
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startLoop()

	return rf
}
//生成400-800ms(400+(0~400))的election interval
func  randElectionInterval() time.Duration{

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	//var interval time.Duration
	// 400~ 800 ms
	timeInterval := r.Int31n(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)+MIN_ELECTION_INTERVAL

	return time.Duration(timeInterval) * time.Millisecond

}
//  Make  下 goroutine 控制节点的state

func (rf *Raft) startLoop(){
	for {
		state := atomic.LoadInt32(&rf.state)
		switch state {
		case FOLLOWER:
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				rf.uptoState(CANDIDATE)
				rf.mu.Unlock()
			case <-rf.voteCh:
				rf.electionTimer.Reset(randElectionInterval())
			case <-rf.appendCh:
				rf.electionTimer.Reset(randElectionInterval())
			}

		case CANDIDATE:

			select {
			case <-rf.electionTimer.C:
				//超时重新election
				rf.startElection()
			case <-rf.appendCh:
				rf.mu.Lock()
				rf.uptoState(FOLLOWER)
				rf.mu.Unlock()
			default:
				// check if it has collected enough vote
				if rf.voteAcquired > len(rf.peers)/2 {
					rf.mu.Lock()
					rf.uptoState(LEADER)
					rf.mu.Unlock()
				}
			}

			//把锁锁在function之外可以保证传入参数的并发安全性，以免多次传入覆盖.
			//锁在函数内部无法锁住函数的传入参数
			//多协程的局部变量共享一个地址空间
		case LEADER:
			//间隔心跳时间广播发送log replication
			rf.broadcastAppendEntries()
			time.Sleep(time.Duration(HEART_INTERVAL)*time.Millisecond)

		}


	}


}

//修改node 状态,必须为这个函数加锁，因为修改函数必须是原子性的

func (rf *Raft) uptoState(newState int32){

	if rf.isState(newState){
		return
	}
	preState := rf.state
	switch newState{
	case FOLLOWER:
		rf.state = FOLLOWER
		rf.votedFor = -1
	case CANDIDATE:
		rf.state = CANDIDATE
		//立即开始选举
		rf.startElection()
	case LEADER:
		rf.state = LEADER
	default:
		fmt.Println("raft.go 307 , Invalid newState in func <uptoState> ")

	}
	//输出转换结果
	fmt.Printf("In Term %d rf State change from %s to %s\n",rf.currentTerm,stateDic[preState],stateDic[newState])

}
//candidate 开始选举

func(rf *Raft) startElection(){
	//reset timer
	rf.electionTimer.Reset(randElectionInterval())
	//term++
	atomic.AddInt32(&rf.currentTerm,1)
	//fmt.Printf("===Term %d ,Starting Election===\n",rf.currentTerm)
	rf.voteAcquired = 1  //给自己增加一个票数
	rf.votedFor = rf.me
	rf.broadcastRV()

}
// candidate  全局广播 request votes
func (rf *Raft) broadcastRV(){
	n := len(rf.peers)
	//fmt.Printf("[]peers.size==%d\n",n)
	//写好票的变量
	args := RequestVoteArgs{Term:rf.currentTerm,CandidateID:rf.me}
	//向除自身以外，所有的节点投放Request Vote ,异步调用RPC
	for i := 0; i<n ; i++ {
		//if 这个节点是本身，那就跳过
		if i == rf.me{
			continue
		}
		//对其余节点，RV
		//注意i 必须写入 否则都是 n-1
		go func(other_node int) {
			var reply RequestVoteReply
			//如果发成功了
			if rf.isState(CANDIDATE) && rf.sendRequestVote(other_node,&args,&reply){
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteOrNot{
					rf.voteAcquired+=1
					//fmt.Printf("In Term %d , rf.voteAcquired=%d \n",rf.currentTerm,rf.voteAcquired)
				}else {
					//candidate 过期了
					if reply.Term > rf.currentTerm{
						rf.uptoState(FOLLOWER)
						rf.votedFor =-1
						rf.voteAcquired = 0
						rf.currentTerm = reply.Term
					}
				}
			}else {
				//fmt.Printf("func <broadcastRV>  send RequestVote to %d node err!\n",other_node)
			}
		}(i)

	}

}
//leader 全局广播append entry

func(rf *Raft) broadcastAppendEntries(){
	n := len(rf.peers)
	//写好append Entry的 args
	args := AppendEntryArgs{Term:rf.currentTerm,LeaderID:rf.me}
	//发起append
	for i:=0; i<n; i++ {
		if i==rf.me {
			continue
		}
		//rpc
		go func(server int) {
			var reply AppendEntryReply
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.sendAppendEntry(server,&args,&reply) {
				if !reply.AppendOrNot{
					//leader要回退到follower
					rf.uptoState(FOLLOWER)
				}
			}
		}(i)

	}




}

