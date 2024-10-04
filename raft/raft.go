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
	votes map[uint64]bool

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
	peersCopy := make([]uint64, len(c.peers))
	copy(peersCopy, c.peers)

	return &Raft{id: c.ID,
		heartbeatTimeout:    c.HeartbeatTick,
		electionTimeout:     c.ElectionTick,
		compElectionTimeout: c.ElectionTick,
		peers:               peersCopy,
		Term:                0,
		Vote:                None,
		Lead:                c.ID,
		electionElapsed:     0,
		heartbeatElapsed:    0,
		State:               StateFollower,
		votes:               make(map[uint64]bool),
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var requestappend pb.Message
	requestappend.MsgType = pb.MessageType_MsgAppend
	requestappend.From = r.id
	requestappend.To = to
	requestappend.Term = r.Term

	for i := range r.RaftLog.unstableEntries() {
		requestappend.Entries = append(requestappend.Entries, &r.RaftLog.unstableEntries()[i])
	}

	return true
}

func (r *Raft) sendAppendResponse(to uint64, m pb.Message) bool {
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
	r.msgs = append(r.msgs, m)
}

// sendvote sends a requestVote RPC to the given peer
func (r *Raft) sendVote(to uint64) {
	var requestvote pb.Message
	requestvote.MsgType = pb.MessageType_MsgRequestVote
	requestvote.Term = r.Term
	requestvote.From = r.id
	requestvote.To = to
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
	r.Vote = None
	r.State = StateFollower
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
	if len(r.votes) != 0 {
		for key := range r.votes {
			delete(r.votes, key)
		}
	}
	r.sendAppend(r.id)
	for _, val := range r.peers {
		if val == r.id {
			continue
		}
		r.sendHeartbeat(val)
	}
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
		} else if m.MsgType == pb.MessageType_MsgRequestVote {
			r.handleVote(m)
		} else if m.MsgType == pb.MessageType_MsgAppend {
			r.handleAppendEntries(m)
		} else if m.MsgType == pb.MessageType_MsgHeartbeatResponse {
			r.handleHeartbeatResponse(m)
		} else if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			r.handleVoteResponse(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateFollower {
		if m.Term < r.Term {
			m.Reject = true
			r.sendAppendResponse(m.From, m)
		} else {
			r.Term = m.Term
			r.Lead = m.From
		}
	} else if r.State == StateCandidate {
		if r.Term <= m.Term {
			r.becomeFollower(m.Term, m.From)
		} else {
			m.Reject = true
		}
	} else if r.State == StateLeader {
		if r.Term < m.Term {
			r.becomeFollower(m.Term, m.From)
		} else {
			m.Reject = true
		}
	}
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if r.State == StateFollower {

	} else if r.State == StateCandidate {
		if r.Term <= m.Term {
			r.becomeFollower(m.Term, m.From)
		} else {

		}
	} else if r.State == StateLeader {
		if r.id == m.From {

		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.State == StateFollower {
		r.heartbeatElapsed = 0
		if r.Term <= m.Term {
			r.Lead = m.From
			r.Vote = None
			r.Term = m.Term
		}
	} else if r.State == StateCandidate {
		if r.Term <= m.Term {
			r.becomeFollower(m.Term, m.From)
		} else {
			m.Reject = true
			r.sendHeartbeatResponse(m.From, m)
		}
	} else if r.State == StateLeader {
		if r.Term >= m.Term {
			m.Reject = true
			r.sendHeartbeatResponse(m.From, m)
		} else {
			r.becomeFollower(m.Term, m.From)
		}
	}
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.State == StateFollower {

	} else if r.State == StateCandidate {

	} else if r.State == StateLeader {
		if m.Reject && m.Term >= r.Term {
			r.becomeFollower(m.Term, m.From)
		}
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

func (r *Raft) handleVote(m pb.Message) {
	if r.State == StateFollower {
		if r.Vote == None || r.Vote == m.From {
			r.Vote = m.From
			r.Term = m.Term
			r.sendVoteResponse(m.From, m)
		} else {
			m.Reject = true
			r.sendVoteResponse(m.From, m)
		}
	} else if r.State == StateCandidate {
		if r.Term >= m.Term {
			m.Reject = true
			r.sendVoteResponse(m.From, m)
		} else {
			r.becomeFollower(m.Term, m.From)
			r.Vote = m.From
			r.sendVoteResponse(m.From, m)
		}

	} else if r.State == StateLeader {
		if r.Term >= m.Term {
			m.Reject = true
			r.sendVoteResponse(m.From, m)
		} else {
			// if r.Vote == None {
			r.becomeFollower(m.Term, m.From)
			r.Vote = m.From
			r.Term = m.Term
			r.sendVoteResponse(m.From, m)
			// } else {
			// 	m.Reject = true
			// 	r.sendVoteResponse(m.From, m)
			// }
		}
	}
}

func (r *Raft) handleVoteResponse(m pb.Message) {
	if r.State == StateFollower {

	} else if r.State == StateCandidate {
		if !m.Reject {
			r.votes[m.From] = true
			if len(r.votes) > len(r.peers)/2 {
				r.becomeLeader()
			}
		}
	} else if r.State == StateLeader {

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
