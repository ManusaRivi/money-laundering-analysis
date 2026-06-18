package protocol

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/google/uuid"
)

// MsgID is the 16-byte deterministic identifier carried by every internal
// envelope. It is the unit of deduplication: a logical message (re)produced
// after a crash or a broker redelivery carries the same MsgID, so a downstream
// consumer can recognise and drop the duplicate.
//
// IDs are never random. A source message is a deterministic function of
// (client, stream, seq); every derived message is a deterministic function of
// its parent's id plus a discriminator. Reprocessing the same input therefore
// regenerates the exact same ids, independent of processing order — which is
// what makes the pipeline idempotent from each consumer's point of view.
type MsgID [16]byte

// Source streams. The gateway keeps an independent sequence per stream, so the
// stream tag keeps their id spaces from colliding.
const (
	StreamTransactions byte = 0
	StreamAccounts     byte = 1
)

// SourceMsgID mints the id of a message born at the gateway. Deterministic in
// (clientID, stream, seq); seq must be monotonic per client+stream.
func SourceMsgID(clientID uuid.UUID, stream byte, seq uint64) MsgID {
	var seqBytes [8]byte
	binary.BigEndian.PutUint64(seqBytes[:], seq)

	buf := make([]byte, 0, len(clientID)+1+len(seqBytes))
	buf = append(buf, clientID[:]...)
	buf = append(buf, stream)
	buf = append(buf, seqBytes[:]...)

	return hash16(buf)
}

// DeriveMsgID mints the id of an output derived from a parent message.
// Deterministic in (parent, discriminator, outIdx): the same inputs always
// yield the same id. The discriminator is length-prefixed so that, e.g.,
// (parent, "ab", n) and (parent, "a", m) can never collide.
func DeriveMsgID(parent MsgID, discriminator string, outIdx uint32) MsgID {
	var discLen [4]byte
	binary.BigEndian.PutUint32(discLen[:], uint32(len(discriminator)))
	var idxBytes [4]byte
	binary.BigEndian.PutUint32(idxBytes[:], outIdx)

	buf := make([]byte, 0, len(parent)+len(discLen)+len(discriminator)+len(idxBytes))
	buf = append(buf, parent[:]...)
	buf = append(buf, discLen[:]...)
	buf = append(buf, discriminator...)
	buf = append(buf, idxBytes[:]...)

	return hash16(buf)
}

// StageMsgID mints the id of a message that isn't tied to a single consumed
// parent — e.g. an aggregator result or an EOF emitted on flush. It is rooted
// in the producing stage (so two stages can't collide) rather than chained from
// a parent. When more than one replica of a stage emits the same logical
// message — because control.eof is broadcast to every replica — the caller must
// fold the replica id into stage, or sibling replicas collide. Deterministic in
// (clientID, stage, discriminator, outIdx).
func StageMsgID(clientID uuid.UUID, stage string, discriminator string, outIdx uint32) MsgID {
	var stageLen [4]byte
	binary.BigEndian.PutUint32(stageLen[:], uint32(len(stage)))
	var discLen [4]byte
	binary.BigEndian.PutUint32(discLen[:], uint32(len(discriminator)))
	var idxBytes [4]byte
	binary.BigEndian.PutUint32(idxBytes[:], outIdx)

	buf := make([]byte, 0, len(clientID)+len(stageLen)+len(stage)+len(discLen)+len(discriminator)+len(idxBytes))
	buf = append(buf, clientID[:]...)
	buf = append(buf, stageLen[:]...)
	buf = append(buf, stage...)
	buf = append(buf, discLen[:]...)
	buf = append(buf, discriminator...)
	buf = append(buf, idxBytes[:]...)
	return hash16(buf)
}

func hash16(buf []byte) MsgID {
	sum := sha256.Sum256(buf)
	var id MsgID
	copy(id[:], sum[:])
	return id
}
