package wire

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"mc714-t2/internal/config"
)

func testCluster() config.Cluster {
	return config.Cluster{
		NodeID:    "node2",
		RunID:     "run-1",
		ClusterID: "cluster-1",
		Members: []config.Member{
			{ID: "node1", URL: "http://node1:8080"},
			{ID: "node2", URL: "http://node2:8080"},
			{ID: "node3", URL: "http://node3:8080"},
		},
		Quorum: 2,
	}
}

func testMessage(kind Kind) Message {
	message := Message{
		Version:   ProtocolVersion,
		RunID:     "run-1",
		ClusterID: "cluster-1",
		From:      "node1",
		To:        "node2",
		MessageID: 1,
		Kind:      kind,
		Request:   RequestID{Owner: "node1", Sequence: 1},
	}
	switch kind {
	case RARequest, RAReply:
		message.Clock = 1
	case ABDState, ABDUpdate, ABDAck:
		message.Register = &Register{Tag: Tag{Counter: 1, WriterID: "node3"}, Value: -42}
	}
	switch kind {
	case RAReply, ABDState, ABDAck:
		message.Request.Owner = message.To
	}
	return message
}

func messageJSON(t *testing.T, message Message) string {
	t.Helper()
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestDecodeMessageRejectsEnvelopeAndContext(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Message, *config.Cluster)
	}{
		{"missing version", func(m *Message, _ *config.Cluster) { m.Version = 0 }},
		{"future version", func(m *Message, _ *config.Cluster) { m.Version = 2 }},
		{"missing run", func(m *Message, _ *config.Cluster) { m.RunID = "" }},
		{"different run", func(m *Message, _ *config.Cluster) { m.RunID = "other" }},
		{"missing cluster", func(m *Message, _ *config.Cluster) { m.ClusterID = "" }},
		{"different cluster", func(m *Message, _ *config.Cluster) { m.ClusterID = "other" }},
		{"missing sender", func(m *Message, _ *config.Cluster) { m.From = "" }},
		{"unknown sender", func(m *Message, _ *config.Cluster) { m.From = "outsider" }},
		{"missing recipient", func(m *Message, _ *config.Cluster) { m.To = "" }},
		{"unknown recipient", func(m *Message, _ *config.Cluster) { m.To = "outsider" }},
		{"other member recipient", func(m *Message, _ *config.Cluster) { m.To = "node3" }},
		{"local node not a member", func(m *Message, c *config.Cluster) {
			c.NodeID = "outsider"
			m.To = c.NodeID
		}},
		{"unknown owner", func(m *Message, _ *config.Cluster) { m.Request.Owner = "outsider" }},
		{"missing owner", func(m *Message, _ *config.Cluster) { m.Request.Owner = "" }},
		{"wrong request owner", func(m *Message, _ *config.Cluster) { m.Request.Owner = "node2" }},
		{"zero message ID", func(m *Message, _ *config.Cluster) { m.MessageID = 0 }},
		{"zero request sequence", func(m *Message, _ *config.Cluster) { m.Request.Sequence = 0 }},
		{"missing kind", func(m *Message, _ *config.Cluster) { m.Kind = "" }},
		{"unknown kind", func(m *Message, _ *config.Cluster) { m.Kind = "ra.unknown" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, cluster := testMessage(RARequest), testCluster()
			test.change(&message, &cluster)
			if _, err := DecodeMessage(strings.NewReader(messageJSON(t, message)), cluster); err == nil {
				t.Fatal("accepted invalid envelope or context")
			}
		})
	}
}

func TestDecodeMessageRejectsPayloadViolations(t *testing.T) {
	tests := []struct {
		name   string
		kind   Kind
		change func(*Message)
	}{
		{"request without clock", RARequest, func(m *Message) { m.Clock = 0 }},
		{"reply without clock", RAReply, func(m *Message) { m.Clock = 0 }},
		{"request with register", RARequest, func(m *Message) { m.Register = &Register{} }},
		{"reply with register", RAReply, func(m *Message) { m.Register = &Register{} }},
		{"request to self", RARequest, func(m *Message) { m.From, m.Request.Owner = m.To, m.To }},
		{"reply to self", RAReply, func(m *Message) { m.From = m.To }},
		{"query with clock", ABDQuery, func(m *Message) { m.Clock = 1 }},
		{"state with clock", ABDState, func(m *Message) { m.Clock = 1 }},
		{"update with clock", ABDUpdate, func(m *Message) { m.Clock = 1 }},
		{"ack with clock", ABDAck, func(m *Message) { m.Clock = 1 }},
		{"query with register", ABDQuery, func(m *Message) { m.Register = &Register{} }},
		{"state without register", ABDState, func(m *Message) { m.Register = nil }},
		{"update without register", ABDUpdate, func(m *Message) { m.Register = nil }},
		{"ack without register", ABDAck, func(m *Message) { m.Register = nil }},
		{"reply owned by sender", RAReply, func(m *Message) { m.Request.Owner = m.From }},
		{"query owned by recipient", ABDQuery, func(m *Message) { m.Request.Owner = m.To }},
		{"state owned by sender", ABDState, func(m *Message) { m.Request.Owner = m.From }},
		{"update owned by recipient", ABDUpdate, func(m *Message) { m.Request.Owner = m.To }},
		{"ack owned by sender", ABDAck, func(m *Message) { m.Request.Owner = m.From }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message := testMessage(test.kind)
			test.change(&message)
			if _, err := DecodeMessage(strings.NewReader(messageJSON(t, message)), testCluster()); err == nil {
				t.Fatal("accepted invalid kind-specific payload")
			}
		})
	}
}

func TestDecodeMessageRegisterConstraints(t *testing.T) {
	tests := []struct {
		name     string
		register Register
		valid    bool
	}{
		{"initial", Register{}, true},
		{"initial nonzero value", Register{Value: 1}, false},
		{"initial negative value", Register{Value: -1}, false},
		{"zero counter with writer", Register{Tag: Tag{WriterID: "node1"}}, false},
		{"positive counter without writer", Register{Tag: Tag{Counter: 1}}, false},
		{"positive counter unknown writer", Register{Tag: Tag{Counter: 1, WriterID: "outsider"}}, false},
		{"positive counter zero value", Register{Tag: Tag{Counter: 1, WriterID: "node3"}}, true},
		{"minimum value", Register{Tag: Tag{Counter: math.MaxUint64, WriterID: "node1"}, Value: math.MinInt64}, true},
		{"maximum value", Register{Tag: Tag{Counter: math.MaxUint64, WriterID: "node2"}, Value: math.MaxInt64}, true},
	}
	for _, kind := range []Kind{ABDState, ABDUpdate, ABDAck} {
		t.Run(string(kind), func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					message := testMessage(kind)
					message.Register = &test.register
					message.MessageID, message.Request.Sequence = math.MaxUint64, math.MaxUint64
					decoded, err := DecodeMessage(strings.NewReader(messageJSON(t, message)), testCluster())
					if !test.valid {
						if err == nil {
							t.Fatal("accepted invalid register")
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					if decoded.Register == nil || *decoded.Register != test.register || decoded.MessageID != math.MaxUint64 || decoded.Request.Sequence != math.MaxUint64 {
						t.Fatal("register or identifier boundary changed during decoding")
					}
				})
			}
		})
	}
}

func TestDecodeMessageAllowsLocalABDReplica(t *testing.T) {
	for _, kind := range []Kind{ABDQuery, ABDState, ABDUpdate, ABDAck} {
		t.Run(string(kind), func(t *testing.T) {
			message := testMessage(kind)
			message.From, message.Request.Owner = message.To, message.To
			if _, err := DecodeMessage(strings.NewReader(messageJSON(t, message)), testCluster()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDecodeMessageRejectsInvalidJSON(t *testing.T) {
	request := messageJSON(t, testMessage(RARequest))
	query := messageJSON(t, testMessage(ABDQuery))
	state := messageJSON(t, testMessage(ABDState))
	tests := []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"null", "null"},
		{"array", "[]"},
		{"truncated", request[:len(request)-1]},
		{"unknown envelope field", strings.TrimSuffix(request, "}") + `,"extra":1}`},
		{"unknown request field", strings.Replace(request, `"sequence":1`, `"sequence":1,"extra":1`, 1)},
		{"unknown register field", strings.Replace(state, `"value":-42`, `"value":-42,"extra":1`, 1)},
		{"unknown tag field", strings.Replace(state, `"writer_id":"node3"`, `"writer_id":"node3","extra":1`, 1)},
		{"trailing object", request + "{}"},
		{"trailing null", request + "null"},
		{"trailing garbage", request + "x"},
		{"ABD explicit zero clock", strings.TrimSuffix(query, "}") + `,"clock":0}`},
		{"ABD explicit null clock", strings.TrimSuffix(query, "}") + `,"clock":null}`},
		{"RA explicit null clock", strings.Replace(request, `"clock":1`, `"clock":null`, 1)},
		{"RA explicit null register", strings.TrimSuffix(request, "}") + `,"register":null}`},
		{"query explicit null register", strings.TrimSuffix(query, "}") + `,"register":null}`},
		{"state explicit null register", strings.Replace(state, `"register":{"tag":{"counter":1,"writer_id":"node3"},"value":-42}`, `"register":null`, 1)},
		{"negative clock", strings.Replace(request, `"clock":1`, `"clock":-1`, 1)},
		{"fractional clock", strings.Replace(request, `"clock":1`, `"clock":1.5`, 1)},
		{"overflow clock", strings.Replace(request, `"clock":1`, `"clock":18446744073709551616`, 1)},
		{"negative message ID", strings.Replace(request, `"message_id":1`, `"message_id":-1`, 1)},
		{"negative sequence", strings.Replace(request, `"sequence":1`, `"sequence":-1`, 1)},
		{"negative counter", strings.Replace(state, `"counter":1`, `"counter":-1`, 1)},
		{"overflow value", strings.Replace(state, `"value":-42`, `"value":9223372036854775808`, 1)},
		{"underflow value", strings.Replace(state, `"value":-42`, `"value":-9223372036854775809`, 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeMessage(strings.NewReader(test.body), testCluster()); err == nil {
				t.Fatal("accepted invalid JSON message")
			}
		})
	}
}

func TestDecodeMessageRejectsIncompleteRegister(t *testing.T) {
	base := messageJSON(t, testMessage(ABDUpdate))
	const original = `"register":{"tag":{"counter":1,"writer_id":"node3"},"value":-42}`
	for _, payload := range []string{
		`{"tag":{"counter":1,"writer_id":"node3"}}`,
		`{"value":0}`,
		`{"tag":{"writer_id":""},"value":0}`,
		`{"tag":{"counter":0},"value":0}`,
		`{"tag":{"counter":0,"writer_id":""},"value":null}`,
	} {
		body := strings.Replace(base, original, `"register":`+payload, 1)
		if _, err := DecodeMessage(strings.NewReader(body), testCluster()); err == nil {
			t.Fatalf("accepted incomplete register: %s", payload)
		}
	}
}

func TestDecodeMessageSizeLimit(t *testing.T) {
	body := messageJSON(t, testMessage(RARequest))
	for _, size := range []int{maxMessageSize - 1, maxMessageSize, maxMessageSize + 1} {
		padded := body + strings.Repeat(" ", size-len(body))
		_, err := DecodeMessage(strings.NewReader(padded), testCluster())
		if (err == nil) != (size <= maxMessageSize) {
			t.Errorf("body size %d: error = %v", size, err)
		}
	}
}

func TestTagCompare(t *testing.T) {
	tests := []struct {
		name        string
		left, right Tag
		want        int
	}{
		{"initial equal", Tag{}, Tag{}, 0},
		{"equal", Tag{Counter: 7, WriterID: "node1"}, Tag{Counter: 7, WriterID: "node1"}, 0},
		{"counter before writer", Tag{Counter: 1, WriterID: "node3"}, Tag{Counter: 2, WriterID: "node1"}, -1},
		{"counter cannot overflow", Tag{Counter: math.MaxUint64, WriterID: "node1"}, Tag{Counter: 1, WriterID: "node3"}, 1},
		{"writer tie break", Tag{Counter: 2, WriterID: "node1"}, Tag{Counter: 2, WriterID: "node3"}, -1},
		{"writer is lexical", Tag{Counter: 2, WriterID: "node10"}, Tag{Counter: 2, WriterID: "node2"}, -1},
		{"initial before written", Tag{}, Tag{Counter: 1, WriterID: "node1"}, -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.left.Compare(test.right); got != test.want {
				t.Errorf("Compare = %d, want %d", got, test.want)
			}
			if got := test.right.Compare(test.left); got != -test.want {
				t.Errorf("reverse Compare = %d, want %d", got, -test.want)
			}
		})
	}
}
