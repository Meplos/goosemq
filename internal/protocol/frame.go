package protocol

import (
	"encoding/json"

	"github.com/google/uuid"
)

type ReqId uuid.UUID
type ReqType string

func (id *ReqId) String() string {
	return id.String()
}

const (
	ACK           ReqType = "ACK"
	PULL          ReqType = "PULL"
	PUSH          ReqType = "PUSH"
	PULL_RESPONSE ReqType = "PULL_RESPONSE"
)

type Frame struct {
	ID      ReqId
	Type    ReqType
	Payload json.RawMessage
}

func NewId() ReqId {
	return ReqId(uuid.New())
}
