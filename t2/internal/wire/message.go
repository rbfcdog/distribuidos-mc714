package wire

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"mc714-t2/internal/config"
)

const ProtocolVersion = 1

const maxMessageSize = 16 * 1024

type Kind string

const (
	RARequest Kind = "ra.request"
	RAReply   Kind = "ra.reply"
	ABDQuery  Kind = "abd.query"
	ABDState  Kind = "abd.state"
	ABDUpdate Kind = "abd.update"
	ABDAck    Kind = "abd.ack"
)

type RequestID struct {
	Owner    string `json:"owner"`
	Sequence uint64 `json:"sequence"`
}

type Tag struct {
	Counter  uint64 `json:"counter"`
	WriterID string `json:"writer_id"`
}

// Compare orders tags by counter, then by writer ID.
func (t Tag) Compare(other Tag) int {
	if order := cmp.Compare(t.Counter, other.Counter); order != 0 {
		return order
	}
	return cmp.Compare(t.WriterID, other.WriterID)
}

type Register struct {
	Tag   Tag   `json:"tag"`
	Value int64 `json:"value"`
}

type Message struct {
	Version   uint8     `json:"version"`
	RunID     string    `json:"run_id"`
	ClusterID string    `json:"cluster_id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	MessageID uint64    `json:"message_id"`
	Kind      Kind      `json:"kind"`
	Request   RequestID `json:"request"`
	Clock     uint64    `json:"clock,omitempty"`
	Register  *Register `json:"register,omitempty"`
}

// DecodeMessage validates one bounded JSON message against static membership.
// It does not track requests, deduplicate messages, or run protocol algorithms.
func DecodeMessage(reader io.Reader, cluster config.Cluster) (Message, error) {
	limited := &io.LimitedReader{R: reader, N: maxMessageSize + 1}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	var envelope struct {
		Message
		Clock    json.RawMessage `json:"clock"`
		Register json.RawMessage `json:"register"`
	}
	if err := decoder.Decode(&envelope); err != nil {
		return Message{}, fmt.Errorf("decode message: %w", err)
	}
	var trailing any
	err := decoder.Decode(&trailing)
	if limited.N == 0 {
		return Message{}, errors.New("message exceeds 16 KiB")
	}
	if err != io.EOF {
		return Message{}, errors.New("message must contain exactly one JSON object")
	}
	message := envelope.Message
	if len(envelope.Clock) != 0 {
		if err := json.Unmarshal(envelope.Clock, &message.Clock); err != nil {
			return Message{}, fmt.Errorf("decode clock: %w", err)
		}
	}
	if len(envelope.Register) != 0 {
		if err := json.Unmarshal(envelope.Register, &message.Register); err != nil {
			return Message{}, fmt.Errorf("decode register: %w", err)
		}
	}
	if err := message.validate(cluster, len(envelope.Clock) != 0, len(envelope.Register) != 0); err != nil {
		return Message{}, err
	}
	return message, nil
}

func (m Message) validate(cluster config.Cluster, hasClock, hasRegister bool) error {
	if m.Version != ProtocolVersion {
		return fmt.Errorf("unsupported protocol version %d", m.Version)
	}
	if m.RunID != cluster.RunID || m.ClusterID != cluster.ClusterID {
		return errors.New("message run or cluster does not match local context")
	}
	if !cluster.HasMember(m.From) || !cluster.HasMember(m.To) || m.To != cluster.NodeID {
		return errors.New("message sender or local recipient is not a matching member")
	}
	if !cluster.HasMember(m.Request.Owner) || m.MessageID == 0 || m.Request.Sequence == 0 {
		return errors.New("message requires a member request owner and positive identifiers")
	}
	switch m.Kind {
	case RARequest, ABDQuery, ABDUpdate:
		if m.Request.Owner != m.From {
			return errors.New("request owner must be the sender")
		}
	case RAReply, ABDState, ABDAck:
		if m.Request.Owner != m.To {
			return errors.New("reply request owner must be the recipient")
		}
	default:
		return fmt.Errorf("unknown message kind %q", m.Kind)
	}

	switch m.Kind {
	case RARequest, RAReply:
		if m.Clock == 0 || hasRegister || m.From == m.To {
			return errors.New("RA requires a positive clock, no register, and distinct members")
		}
	case ABDQuery, ABDState, ABDUpdate, ABDAck:
		if hasClock {
			return errors.New("ABD forbids a clock field")
		}
		if m.Kind == ABDQuery {
			if hasRegister {
				return errors.New("ABD query forbids a register")
			}
			return nil
		}
		if m.Register == nil {
			return errors.New("ABD state, update, and ack require a register")
		}
		return ValidateRegister(*m.Register, cluster)
	}
	return nil
}
