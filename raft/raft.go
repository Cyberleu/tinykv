// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64
	//ifparticipatevote bool

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes       map[uint64]bool
	rejectCount uint64

	// msgs need to send
	msgs []pb.Message

	//peers
	peers []uint64

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout     int
	compElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, _ := c.Storage.InitialState()
	peersCopy := make([]uint64, len(c.peers))
	copy(peersCopy, c.peers)
	if peersCopy == nil {
		peersCopy = confState.Nodes
	}

	prsCopy := make(map[uint64]*Progress)

	r := &Raft{id: c.ID,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		compElectionTimeout: c.ElectionTick,
		peers:               peersCopy,
		Term:                hardState.Term,
		Vote:                hardState.Vote,
		Lead:                None,
		electionElapsed:     0,
		heartbeatElapsed:    0,
		State:               StateFollower,
		votes:               make(map[uint64]bool),
		rejectCount:         0,
		Prs:                 prsCopy,
		RaftLog:             newLog(c.Storage),
		msgs:                make([]pb.Message, 0),
	}
	lastIndex := r.RaftLog.LastIndex()
	for _, val := range peersCopy {
		if r.id == val {
			r.Prs[val] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		} else {
			r.Prs[val] = &Progress{Match: 0, Next: lastIndex + 1}
		}
	}
	r.becomeFollower(0, None)
	return r
}

func (r *Raft) appendEntries(entries ...*pb.Entry) {
	logEntries := make([]pb.Entry, 0)
	for _, curr := range entries {
		logEntries = append(logEntries, pb.Entry{
			EntryType: curr.EntryType,
			Term:      curr.Term,
			Index:     curr.Index,
			Data:      curr.Data,
		})
	}
	r.RaftLog.appendEntries(logEntries...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

func (r *Raft) updateCommit() {
	flag := false
	for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		match := 0
		for _, rp := range r.Prs {
			if rp.Match >= i {
				match++
			}
		}
		term, _ := r.RaftLog.Term(i)
		if match > len(r.Prs)/2 && term == r.Term && r.RaftLog.committed != i {
			r.RaftLog.committed = i
			flag = true
		}
	}
	if flag {
		for _, id := range r.peers {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	lastIndex := r.RaftLog.LastIndex()
	prevLogIndex := r.Prs[to].Next - 1
	// if lastIndex < prevLogIndex {
	// 	return true
	// }
	prevLogTerm, _ := r.RaftLog.Term(prevLogIndex)
	logEntries := r.RaftLog.Entries(prevLogIndex+1, lastIndex+1)
	sendEntries := make([]*pb.Entry, 0)
	for _, curr := range logEntries {
		sendEntries = append(sendEntries, &pb.Entry{
			EntryType: curr.EntryType,
			Term:      curr.Term,
			Index:     curr.Index,
			Data:      curr.Data,
		})
	}
	var requestappend pb.Message
	requestappend.MsgType = pb.MessageType_MsgAppend
	requestappend.From = r.id
	requestappend.To = to
	requestappend.Term = r.Term
	requestappend.Commit = r.RaftLog.committed
	requestappend.LogTerm = prevLogTerm
	requestappend.Index = prevLogIndex
	requestappend.Entries = sendEntries
	r.msgs = append(r.msgs, requestappend)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, m pb.Message) bool {
	m.Term = r.Term
	m.Index = r.RaftLog.LastIndex()
	m.To = to
	m.From = r.id
	m.MsgType = pb.MessageType_MsgAppendResponse
	r.msgs = append(r.msgs, m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	var requesthearbeat pb.Message
	requesthearbeat.MsgType = pb.MessageType_MsgHeartbeat
	requesthearbeat.From = r.id
	requesthearbeat.To = to
	requesthearbeat.Term = r.Term
	r.msgs = append(r.msgs, requesthearbeat)
}

func (r *Raft) sendHeartbeatResponse(to uint64, m pb.Message) {
	m.MsgType = pb.MessageType_MsgHeartbeatResponse
	m.From = r.id
	m.To = to
	m.Term = r.Term
	m.Index = r.RaftLog.LastIndex()
	r.msgs = append(r.msgs, m)
}

// sendvote sends a requestVote RPC to the given peer
func (r *Raft) sendVote(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	var requestvote pb.Message
	requestvote.MsgType = pb.MessageType_MsgRequestVote
	requestvote.Term = r.Term
	requestvote.From = r.id
	requestvote.To = to
	requestvote.LogTerm = lastTerm
	requestvote.Index = lastIndex
	r.msgs = append(r.msgs, requestvote)
}

func (r *Raft) sendVoteResponse(to uint64, m pb.Message) {
	m.MsgType = pb.MessageType_MsgRequestVoteResponse
	m.From = r.id
	m.To = to
	m.Term = r.Term
	r.msgs = append(r.msgs, m)
}

func (r *Raft) sendHup(to uint64) {
	var requesthup pb.Message
	requesthup.MsgType = pb.MessageType_MsgHup
	requesthup.From = r.id
	requesthup.To = to
	requesthup.Term = r.Term
	r.msgs = append(r.msgs, requesthup)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateFollower {
		r.electionElapsed++
		if r.electionElapsed >= r.compElectionTimeout {
			r.becomeCandidate()
			for _, val := range r.peers {
				if val == r.id {
					continue
				}
				r.sendVote(val)
			}
		}
	} else if r.State == StateCandidate {
		r.electionElapsed++
		if r.electionElapsed >= r.compElectionTimeout {
			r.becomeCandidate()
			for _, val := range r.peers {
				if val == r.id {
					continue
				}
				r.sendVote(val)
			}
		}
	} else if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			for _, val := range r.peers {
				if val == r.id {
					continue
				}
				r.sendHeartbeat(val)
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// r.electionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.compElectionTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.rejectCount = 0
	if len(r.votes) != 0 {
		for key := range r.votes {
			delete(r.votes, key)
		}
	}
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes[r.id] = true
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// r.electionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.compElectionTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
	if len(r.votes) > len(r.peers)/2 {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// r.electionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.compElectionTimeout = rand.Intn(r.electionTimeout) + r.electionTimeout
	r.Lead = r.id
	r.Vote = None
	r.rejectCount = 0
	if len(r.votes) != 0 {
		for key := range r.votes {
			delete(r.votes, key)
		}
	}
	r.appendEntries(&pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
		Data:      nil,
	})
	for _, id := range r.peers {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
	r.updateCommit()
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		} else if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleVote(m)
		} else if m.MsgType == pb.MessageType_MsgHup {
			r.handleHup(m)
		} else if m.MsgType == pb.MessageType_MsgHeartbeatResponse {

		} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			r.handleVoteResponse(m)
		}
	case StateCandidate:
		if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleVote(m)
		} else if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		} else if m.MsgType == pb.MessageType_MsgHup {
			r.handleHup(m)
		} else if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
			r.handleHeartbeatResponse(m)
		} else if m.MsgType == pb.MessageType_MsgAppendResponse {
			r.handleAppendEntriesResponse(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			r.handleVoteResponse(m)
		}
	case StateLeader:
		if m.MsgType == pb.MessageType_MsgHeartbeat {
			r.handleHeartbeat(m)
		} else if m.MsgType == pb.MessageType_MsgBeat {
			r.handleBeat(m)
		} else if m.MsgType == pb.MessageType_MsgPropose {
			r.handlePropose(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleVote(m)
		} else if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		} else if m.MsgType == pb.MessageType_MsgAppendResponse {
			r.handleAppendEntriesResponse(m)
		} else if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
			r.handleHeartbeatResponse(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			r.handleVoteResponse(m)
		}
	}
	return nil
}

func (r *Raft) handlePropose(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	logEntries := make([]*pb.Entry, 0)
	for _, logEntry := range m.Entries {
		logEntries = append(logEntries, &pb.Entry{
			EntryType: logEntry.EntryType,
			Term:      r.Term,
			Index:     lastIndex + 1,
			Data:      logEntry.Data,
		})
		lastIndex++
	}
	r.appendEntries(logEntries...)
	for _, val := range r.peers {
		if val == r.id {
			continue
		}
		r.sendAppend(val)
	}
	r.updateCommit()
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).

	if m.Term < r.Term {
		m.Reject = true
		r.sendAppendResponse(m.From, m)
	} else {
		r.becomeFollower(m.Term, m.From)
		logTerm, err := r.RaftLog.Term(m.Index)
		if err != nil || logTerm != m.LogTerm {
			m.Reject = true
			r.sendAppendResponse(m.From, m)
			return
		}
		if len(m.Entries) > 0 {
			start := 0
			for idx, logEntry := range m.Entries {
				if logEntry.Index > r.RaftLog.LastIndex() {
					start = idx
					break
				}
				validTerm, _ := r.RaftLog.Term(logEntry.Index)
				if validTerm != logEntry.Term {
					r.RaftLog.remove(logEntry.Index)
					break
				}
				start = idx
			}
			if m.Entries[start].Index > r.RaftLog.LastIndex() {
				for _, logEntry := range m.Entries[start:] {
					r.RaftLog.entries = append(r.RaftLog.entries, *logEntry)
				}
			}
		}
		if m.Commit >= r.RaftLog.committed {
			lastNewEntry := m.Index
			if len(m.Entries) > 0 {
				lastNewEntry = m.Entries[len(m.Entries)-1].Index
			}
			r.RaftLog.committed = min(m.Commit, lastNewEntry)
		}
		r.sendAppendResponse(m.From, m)
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if !m.Reject {
		r.Prs[m.From].Next = m.Index + 1
		r.Prs[m.From].Match = m.Index
	} else if r.Prs[m.From].Next > 0 {
		r.Prs[m.From].Next--
		r.sendAppend(m.From)
		return
	}
	r.updateCommit()
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		m.Reject = true
		r.sendHeartbeatResponse(m.From, m)
		return
	}
	r.becomeFollower(m.Term, m.From)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		m.Reject = true
		r.sendHeartbeatResponse(m.From, m)
		return
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendHeartbeatResponse(m.From, m)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if r.compLogUpdate(m.LogTerm, m.Index) {
		r.sendAppend(m.From)
	}

}

func (r *Raft) handleBeat(m pb.Message) {
	for _, val := range r.peers {
		if val == r.id {
			continue
		}
		r.sendHeartbeat(val)
	}
}

func (r *Raft) compLogUpdate(logTerm, index uint64) bool {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if lastTerm > logTerm || (lastTerm == logTerm && r.RaftLog.LastIndex() > index) {
		return true
	}
	return false
}

func (r *Raft) handleVote(m pb.Message) {
	if r.Term > m.Term {
		m.Reject = true
		r.sendVoteResponse(m.From, m)
		return
	}
	if r.compLogUpdate(m.LogTerm, m.Index) {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		m.Reject = true
		r.sendVoteResponse(m.From, m)
		return
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		r.sendVoteResponse(m.From, m)
		return
	}
	if r.Vote == m.From {
		r.sendVoteResponse(m.From, m)
		return
	}
	if r.State == StateFollower && r.Vote == None {
		lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		if lastTerm < m.LogTerm || (lastTerm == m.LogTerm && m.Index >= r.RaftLog.LastIndex()) {
			r.sendVoteResponse(m.From, m)
			return
		}
	}
	r.compElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	m.Reject = true
	r.sendVoteResponse(m.From, m)
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, r.Lead)
		r.Vote = m.From
		return
	}
	if !m.Reject {
		r.votes[m.From] = true
		if len(r.votes) > len(r.peers)/2 {
			r.becomeLeader()
		}
	} else {
		r.rejectCount++
		if r.rejectCount > uint64(len(r.peers)/2) {
			r.becomeFollower(r.Term, r.Lead)
		}
	}
}

func (r *Raft) handleHup(m pb.Message) {
	r.becomeCandidate()
	for _, val := range r.peers {
		if val == r.id {
			continue
		}
		r.sendVote(val)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
