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

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes       map[uint64]bool
	acCount     int
	rejectCount int

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// real election timeout time.
	// random between et and 2*et
	realElectionTimeout int
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
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		Vote:             hardState.Vote,
		Term:             hardState.Term,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		State:            StateFollower,
		votes:            make(map[uint64]bool),
		acCount:          0,
		rejectCount:      0,
		msgs:             make([]pb.Message, 0),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}
	lastIndex := r.RaftLog.LastIndex()
	if c.peers == nil {
		c.peers = confState.Nodes
	}
	r.Prs[r.id] = &Progress{Match: lastIndex, Next: lastIndex + 1}
	for _, id := range c.peers {
		if id == r.id {
			r.Prs[id] = &Progress{Match: lastIndex, Next: lastIndex + 1}
		} else {
			r.Prs[id] = &Progress{Match: 0, Next: lastIndex + 1}
		}
	}
	r.becomeFollower(0, None)
	return r
}

func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed == r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.realElectionTimeout {
			r.electionElapsed = 0
			r.vote()
		}
	}
}

func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.votes = nil
	r.acCount = 0
	r.rejectCount = 0
	r.leadTransferee = None
	r.resetTick()
}

func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.acCount = 1
	r.resetTick()
	if r.acCount > len(r.Prs)/2 {
		r.becomeLeader()
	}
}

func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	lastIndex := r.RaftLog.LastIndex() + 1
	for _, v := range r.Prs {
		v.Match = 0
		v.Next = lastIndex
	}
	r.resetTick()
	r.appendEntries(&pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     lastIndex,
		Data:      nil,
	})
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
	r.updateCommit()
}

func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if _, ok := r.Prs[r.id]; !ok {
		return nil
	}
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.vote()
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTimeoutNow:
			r.vote()
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		}
		return nil
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.vote()
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			if r.Lead != None {
				m.To = r.Lead
				r.msgs = append(r.msgs, m)
			}
		}
		return nil
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			lastIndex := r.RaftLog.LastIndex()
			ents := make([]*pb.Entry, 0)
			for _, e := range m.Entries {
				ents = append(ents, &pb.Entry{
					EntryType: e.EntryType,
					Term:      r.Term,
					Index:     lastIndex + 1,
					Data:      e.Data,
				})
				lastIndex++
			}
			r.appendEntries(ents...)
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendAppend(id)
			}
			r.updateCommit()
		case pb.MessageType_MsgRequestVote:
			r.handleVoteRequest(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgBeat:
			for id := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResponse(m)
		case pb.MessageType_MsgHeartbeatResponse:
			r.handleHeartbeatResponse(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgTransferLeader:
			r.handleLeaderTransfer(m)
		}
		return nil
	}
	return nil
}

func (r *Raft) vote() {
	r.becomeCandidate()
	r.sendVote()
}

func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		if err == ErrCompacted {
			r.sendAppendResponse(m.From, false)
			return
		}
		r.sendAppendResponse(m.From, true)
		return
	}
	if len(m.Entries) > 0 {
		st := 0
		for i, logEntry := range m.Entries {
			if logEntry.Index > r.RaftLog.LastIndex() {
				st = i
				break
			}
			validTerm, _ := r.RaftLog.Term(logEntry.Index)
			if validTerm != logEntry.Term {
				r.RaftLog.remove(logEntry.Index)
				break
			}
			st = i
		}
		if m.Entries[st].Index > r.RaftLog.LastIndex() {
			for _, logEntry := range m.Entries[st:] {
				r.RaftLog.entries = append(r.RaftLog.entries, *logEntry)
			}
		}
	}
	if m.Commit > r.RaftLog.committed {
		newEntryIndex := m.Index
		if len(m.Entries) > 0 {
			newEntryIndex = m.Entries[len(m.Entries)-1].Index
		}
		r.RaftLog.committed = min(m.Commit, newEntryIndex)
	}
	r.sendAppendResponse(m.From, false)
}

func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.becomeFollower(m.Term, m.From)
	term, err := r.RaftLog.Term(m.Index)
	if err != nil || term != m.LogTerm {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
	}
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	if m.Snapshot.Metadata.Index < r.RaftLog.committed {
		r.sendAppendResponse(m.From, false)
		return
	}
	r.becomeFollower(m.Term, m.From)
	r.Term = m.Snapshot.Metadata.Term
	r.RaftLog.committed = m.Snapshot.Metadata.Index
	r.RaftLog.applied = m.Snapshot.Metadata.Index
	r.RaftLog.stabled = m.Snapshot.Metadata.Index
	r.RaftLog.firstIndex = m.Snapshot.Metadata.Index + 1
	r.RaftLog.entries = nil
	r.RaftLog.pendingSnapshot = m.Snapshot
	nodes := m.Snapshot.Metadata.ConfState.Nodes
	r.Prs = make(map[uint64]*Progress)
	for _, id := range nodes {
		if id == r.id {
			r.Prs[id] = &Progress{Match: r.RaftLog.committed, Next: r.RaftLog.committed + 1}
		} else {
			r.Prs[id] = &Progress{Match: 0, Next: r.RaftLog.committed + 1}
		}
	}
	r.sendAppendResponse(m.From, false)
}

func (r *Raft) handleVoteRequest(m pb.Message) {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if r.Term > m.Term {
		r.sendVoteResponse(m.From, true)
		return
	}
	if r.compareLog(m.LogTerm, m.Index) {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, None)
		}
		r.sendVoteResponse(m.From, true)
		return
	}
	if r.Term < m.Term {
		r.becomeFollower(m.Term, None)
		r.Vote = m.From
		r.sendVoteResponse(m.From, false)
		return
	}
	if r.Vote == m.From {
		r.sendVoteResponse(m.From, false)
		return
	}
	if r.State == StateFollower &&
		r.Vote == None &&
		(lastTerm < m.LogTerm || (lastTerm == m.LogTerm && m.Index >= r.RaftLog.LastIndex())) {
		r.sendVoteResponse(m.From, false)
		return
	}
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendVoteResponse(m.From, true)
}

func (r *Raft) compareLog(logTerm, index uint64) bool {
	lastTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
	if lastTerm > logTerm || (lastTerm == logTerm && r.RaftLog.LastIndex() > index) {
		return true
	}
	return false
}

func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  1,
		}
	}
	r.PendingConfIndex = None
}

func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		if r.State == StateLeader {
			r.updateCommit()
		}
	}
	r.PendingConfIndex = None
}

func (r *Raft) resetTick() {
	r.heartbeatElapsed = 0
	r.electionElapsed = 0
	r.realElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) appendEntries(entries ...*pb.Entry) {
	logEntries := make([]pb.Entry, 0)
	for _, logEntry := range entries {
		if logEntry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex != None {
				continue
			}
			r.PendingConfIndex = logEntry.Index
		}
		logEntries = append(logEntries, pb.Entry{
			EntryType: logEntry.EntryType,
			Term:      logEntry.Term,
			Index:     logEntry.Index,
			Data:      logEntry.Data,
		})
	}
	r.RaftLog.appendEntries(logEntries...)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

func (r *Raft) sendVoteResponse(nvote uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      nvote,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendAppendResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendVote() {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	for id := range r.Prs {
		if id != r.id {
			msg := pb.Message{
				MsgType: pb.MessageType_MsgRequestVote,
				From:    r.id,
				To:      id,
				Term:    r.Term,
				LogTerm: lastTerm,
				Index:   lastIndex,
			}
			r.msgs = append(r.msgs, msg)
		}
	}
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, r.Lead)
		r.Vote = m.From
		return
	}
	if !m.Reject {
		r.votes[m.From] = true
		r.acCount++
	} else {
		r.votes[m.From] = false
		r.rejectCount++
	}
	if r.acCount > len(r.Prs)/2 {
		r.becomeLeader()
	} else if r.rejectCount > len(r.Prs)/2 {
		r.becomeFollower(r.Term, r.Lead)
	}
}

func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	lastIndex := r.RaftLog.LastIndex()
	preLogIndex := r.Prs[to].Next - 1
	if lastIndex < preLogIndex {
		return true
	}
	preLogTerm, err := r.RaftLog.Term(preLogIndex)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
			return false
		}
		return false
	}
	entries := r.RaftLog.Entries(preLogIndex+1, lastIndex+1)
	sendEntreis := make([]*pb.Entry, 0)
	for _, en := range entries {
		sendEntreis = append(sendEntreis, &pb.Entry{
			EntryType: en.EntryType,
			Term:      en.Term,
			Index:     en.Index,
			Data:      en.Data,
		})
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: preLogTerm,
		Index:   preLogIndex,
		Entries: sendEntreis,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term > r.Term {
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
	if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		return
	}
	if r.compareLog(m.LogTerm, m.Index) {
		r.sendAppend(m.From)
	}
}

func (r *Raft) updateCommit() {
	flag := false
	for i := r.RaftLog.committed + 1; i <= r.RaftLog.LastIndex(); i++ {
		cnt := 0
		for _, p := range r.Prs {
			if p.Match >= i {
				cnt++
			}
		}
		term, _ := r.RaftLog.Term(i)
		if cnt > len(r.Prs)/2 && term == r.Term && r.RaftLog.committed != i {
			r.RaftLog.committed = i
			flag = true
		}
	}
	if flag {
		for id := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
	}
}

func (r *Raft) sendSnapshot(to uint64) {
	snap, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		return
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snap,
	})
	r.Prs[to].Next = snap.Metadata.Index + 1
}

func (r *Raft) handleLeaderTransfer(m pb.Message) {
	if m.From == r.id {
		return
	}
	if r.leadTransferee == m.From {
		return
	}
	if _, ok := r.Prs[m.From]; !ok {
		return
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match != r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	} else {
		r.sendTimeoutNow(m.From)
	}
}

func (r *Raft) sendTimeoutNow(to uint64) {
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
	})
}
