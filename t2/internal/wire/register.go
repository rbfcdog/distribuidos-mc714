package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"mc714-t2/internal/config"
)

// UnmarshalJSON requires every register field, including meaningful zero values.
func (r *Register) UnmarshalJSON(data []byte) error {
	var payload struct {
		Tag *struct {
			Counter  *uint64 `json:"counter"`
			WriterID *string `json:"writer_id"`
		} `json:"tag"`
		Value *int64 `json:"value"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("register must contain exactly one JSON object")
	}
	if payload.Tag == nil || payload.Tag.Counter == nil || payload.Tag.WriterID == nil || payload.Value == nil {
		return errors.New("register requires tag.counter, tag.writer_id and value")
	}
	*r = Register{Tag: Tag{Counter: *payload.Tag.Counter, WriterID: *payload.Tag.WriterID}, Value: *payload.Value}
	return nil
}

// ValidateRegister enforces the initial state and the configured writer namespace.
func ValidateRegister(register Register, cluster config.Cluster) error {
	if register.Tag.Counter == 0 {
		if register.Tag.WriterID != "" || register.Value != 0 {
			return errors.New("initial register must have an empty writer and zero value")
		}
	} else if !cluster.HasMember(register.Tag.WriterID) {
		return errors.New("positive register tag requires a member writer")
	}
	return nil
}
