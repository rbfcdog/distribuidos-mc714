// Package config loads the static membership shared by every node in a run.
package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
)

const maxConfigBytes = 64 << 10

const (
	Standalone = "standalone"
	Integrated = "integrated"
)

type Member struct {
	ID  string `json:"id"`
	URL string `json:"url"`
}

type Cluster struct {
	NodeID    string   `json:"node_id"`
	RunID     string   `json:"run_id"`
	Mode      string   `json:"mode"`
	ClusterID string   `json:"cluster_id"`
	Members   []Member `json:"members"`
	Quorum    int      `json:"quorum"`
}

// Load validates membership before returning a normalized, run-scoped cluster.
// ClusterID hashes run_id, mode and members sorted by ID, so peers with different
// application write policies cannot accidentally form one protocol cluster.
func Load(path, nodeID, runID string) (Cluster, error) {
	if !validID(nodeID) || !validID(runID) {
		return Cluster{}, fmt.Errorf("node ID and run ID must contain 1..64 ASCII letters, digits, underscores or hyphens")
	}
	file, err := os.Open(path)
	if err != nil {
		return Cluster{}, fmt.Errorf("open membership: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxConfigBytes+1))
	if err != nil {
		return Cluster{}, fmt.Errorf("read membership: %w", err)
	}
	if len(data) > maxConfigBytes {
		return Cluster{}, fmt.Errorf("membership exceeds %d bytes", maxConfigBytes)
	}
	members, mode, err := decodeMembers(data)
	if err != nil {
		return Cluster{}, fmt.Errorf("decode membership: %w", err)
	}
	if len(members) != 3 && len(members) != 5 && len(members) != 7 {
		return Cluster{}, fmt.Errorf("membership must contain 3, 5 or 7 members")
	}
	ids := make(map[string]bool, len(members))
	urls := make(map[string]bool, len(members))
	for i := range members {
		member := &members[i]
		if !validID(member.ID) || ids[member.ID] {
			return Cluster{}, fmt.Errorf("invalid or duplicate member ID %q", member.ID)
		}
		ids[member.ID] = true
		canonical, err := canonicalURL(member.URL)
		if err != nil {
			return Cluster{}, fmt.Errorf("member %q: %w", member.ID, err)
		}
		if urls[canonical] {
			return Cluster{}, fmt.Errorf("duplicate member URL %q", canonical)
		}
		urls[canonical] = true
		member.URL = canonical
	}
	if !ids[nodeID] {
		return Cluster{}, fmt.Errorf("node ID %q is not a member", nodeID)
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	identity, err := json.Marshal(struct {
		RunID   string   `json:"run_id"`
		Mode    string   `json:"mode"`
		Members []Member `json:"members"`
	}{RunID: runID, Mode: mode, Members: members})
	if err != nil {
		return Cluster{}, fmt.Errorf("encode cluster identity: %w", err)
	}
	digest := sha256.Sum256(identity)
	return Cluster{
		NodeID: nodeID, RunID: runID, Mode: mode, ClusterID: hex.EncodeToString(digest[:]),
		Members: members, Quorum: len(members)/2 + 1,
	}, nil
}

func (c Cluster) HasMember(id string) bool {
	for _, member := range c.Members {
		if member.ID == id {
			return true
		}
	}
	return false
}

func validID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// Read the exact schema with tokens so duplicate keys and case aliases cannot
// silently replace membership or identities, as encoding/json normally permits.
func decodeMembers(data []byte) ([]Member, string, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := expectDelimiter(dec, '{'); err != nil {
		return nil, "", err
	}
	var mode string
	var members []Member
	var modeSeen, membersSeen bool
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return nil, "", err
		}
		switch key {
		case "mode":
			if modeSeen {
				return nil, "", fmt.Errorf("duplicate mode field")
			}
			modeSeen = true
			err = dec.Decode(&mode)
		case "members":
			if membersSeen {
				return nil, "", fmt.Errorf("duplicate members field")
			}
			membersSeen = true
			members, err = decodeMemberList(dec)
		default:
			return nil, "", fmt.Errorf("unknown membership field %q", key)
		}
		if err != nil {
			return nil, "", err
		}
	}
	if err := expectDelimiter(dec, '}'); err != nil {
		return nil, "", err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, "", fmt.Errorf("expected end of membership JSON")
	}
	if !modeSeen || !membersSeen || (mode != Standalone && mode != Integrated) {
		return nil, "", fmt.Errorf("mode must be standalone or integrated, and members must be present")
	}
	return members, mode, nil
}

func decodeMemberList(dec *json.Decoder) ([]Member, error) {
	if err := expectDelimiter(dec, '['); err != nil {
		return nil, err
	}
	members := make([]Member, 0, 7)
	for dec.More() {
		if len(members) == 7 {
			return nil, fmt.Errorf("membership exceeds seven members")
		}
		if err := expectDelimiter(dec, '{'); err != nil {
			return nil, err
		}
		var member Member
		var idSeen, urlSeen bool
		for dec.More() {
			key, err := dec.Token()
			if err != nil {
				return nil, err
			}
			switch key {
			case "id":
				if idSeen {
					return nil, fmt.Errorf("duplicate member id field")
				}
				idSeen = true
				err = dec.Decode(&member.ID)
			case "url":
				if urlSeen {
					return nil, fmt.Errorf("duplicate member url field")
				}
				urlSeen = true
				err = dec.Decode(&member.URL)
			default:
				return nil, fmt.Errorf("unknown member field %q", key)
			}
			if err != nil {
				return nil, err
			}
		}
		if err := expectDelimiter(dec, '}'); err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	if err := expectDelimiter(dec, ']'); err != nil {
		return nil, err
	}
	return members, nil
}

func expectDelimiter(dec *json.Decoder, want json.Delim) error {
	got, err := dec.Token()
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("expected %q, got %v", want, got)
	}
	return nil
}

func canonicalURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("invalid member URL: %w", err)
	}
	if !strings.EqualFold(u.Scheme, "http") || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(raw, "#") || u.RawPath != "" || u.Path != "" && u.Path != "/" {
		return "", fmt.Errorf("member URL must be HTTP with no credentials, query, fragment or non-root path")
	}
	host, portText, err := net.SplitHostPort(u.Host)
	if err != nil || host == "" || portText == "" {
		return "", fmt.Errorf("member URL must have a host and explicit numeric port")
	}
	for i := range len(portText) {
		if portText[i] < '0' || portText[i] > '9' {
			return "", fmt.Errorf("member URL port must be numeric")
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("member URL port must be in 1..65535")
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		if strings.HasPrefix(u.Host, "[") && !addr.Is6() {
			return "", fmt.Errorf("member URL brackets require an IPv6 address")
		}
		if addr.Zone() != "" {
			return "", fmt.Errorf("member URL cannot use a scoped IP address")
		}
		host = addr.Unmap().String()
	} else {
		host = strings.TrimSuffix(strings.ToLower(host), ".")
		if !validHostname(host) || strings.Contains(u.Host, "[") || strings.Contains(u.Host, "]") {
			return "", fmt.Errorf("invalid member URL host %q", host)
		}
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(port)), nil
}

func validHostname(host string) bool {
	name := host
	if name == "" || len(name) > 253 {
		return false
	}
	numeric := true
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := range len(label) {
			c := label[i]
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
			if c < '0' || c > '9' {
				numeric = false
			}
		}
	}
	// Numeric-only hosts must pass the strict IP parser, not DNS fallback.
	return !numeric
}
