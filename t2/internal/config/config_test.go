package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const membershipJSON = `{"mode":"standalone","members":[{"id":"node1","url":"http://node1:8080"},{"id":"node2","url":"http://node2:8080"},{"id":"node3","url":"http://node3:8080"}]}`

func loadJSON(t *testing.T, body, nodeID, runID string) (Cluster, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "members.json")
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return Load(path, nodeID, runID)
}

func TestClusterIdentity(t *testing.T) {
	first, err := loadJSON(t, membershipJSON, "node1", "run_1")
	if err != nil {
		t.Fatal(err)
	}
	const reordered = `{"members":[{"url":"http://NODE3:08080/","id":"node3"},{"id":"node1","url":"http://NODE1:8080/"},{"id":"node2","url":"http://node2:8080"}],"mode":"standalone"}`
	second, err := loadJSON(t, reordered, "node3", "run_1")
	if err != nil {
		t.Fatal(err)
	}
	if second.ClusterID != first.ClusterID {
		t.Fatalf("unstable or noncanonical identity: %q, %q", first.ClusterID, second.ClusterID)
	}
	for i, member := range second.Members {
		want := fmt.Sprintf("node%d", i+1)
		if member.ID != want || member.URL != "http://"+want+":8080" {
			t.Fatalf("member %d is not sorted and canonical: %+v", i, member)
		}
	}
	if !second.HasMember("node1") || second.HasMember("missing") {
		t.Fatal("HasMember does not reflect membership")
	}
	for name, body := range map[string]string{
		"endpoint": strings.Replace(membershipJSON, "node1:8080", "node1:8081", 1),
		"identity": strings.Replace(membershipJSON, `"node2"`, `"other"`, 1),
		"mode":     strings.Replace(membershipJSON, `"standalone"`, `"integrated"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			changed, err := loadJSON(t, body, "node1", "run_1")
			if err != nil || changed.ClusterID == first.ClusterID {
				t.Fatalf("membership change did not change identity: %+v, %v", changed, err)
			}
		})
	}
	changed, err := loadJSON(t, membershipJSON, "node1", "run_2")
	if err != nil || changed.ClusterID == first.ClusterID {
		t.Fatalf("run change did not change identity: %+v, %v", changed, err)
	}
}

func TestMembershipSizes(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 4, 5, 6, 7, 8} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			members := make([]Member, size)
			for i := range members {
				members[i] = Member{ID: fmt.Sprintf("node%d", i+1), URL: fmt.Sprintf("http://node%d:8080", i+1)}
			}
			body, err := json.Marshal(struct {
				Mode    string   `json:"mode"`
				Members []Member `json:"members"`
			}{Mode: Standalone, Members: members})
			if err != nil {
				t.Fatal(err)
			}
			cluster, err := loadJSON(t, string(body), "node1", "run")
			valid := size == 3 || size == 5 || size == 7
			if valid && (err != nil || cluster.Quorum != size/2+1) {
				t.Fatalf("valid membership: quorum=%d, error=%v", cluster.Quorum, err)
			}
			if !valid && err == nil {
				t.Fatal("invalid membership size accepted")
			}
		})
	}
}

func TestRejectsInvalidConfiguration(t *testing.T) {
	for name, body := range map[string]string{
		"empty":                      "",
		"null":                       "null",
		"array":                      "[]",
		"missing members":            `{}`,
		"null members":               `{"mode":"standalone","members":null}`,
		"unknown field":              strings.Replace(membershipJSON, `"members":`, `"extra":true,"members":`, 1),
		"unknown member":             strings.Replace(membershipJSON, `"id":"node1"`, `"extra":true,"id":"node1"`, 1),
		"duplicate members":          strings.TrimSuffix(membershipJSON, "}") + `,"members":[]}`,
		"duplicate id key":           strings.Replace(membershipJSON, `"id":"node1"`, `"id":"other","id":"node1"`, 1),
		"duplicate url key":          strings.Replace(membershipJSON, `"url":"http://node1:8080"`, `"url":"http://other:8080","url":"http://node1:8080"`, 1),
		"case alias":                 strings.Replace(membershipJSON, `"members"`, `"Members"`, 1),
		"missing mode":               strings.Replace(membershipJSON, `"mode":"standalone",`, "", 1),
		"unknown mode":               strings.Replace(membershipJSON, `"standalone"`, `"other"`, 1),
		"null mode":                  strings.Replace(membershipJSON, `"standalone"`, `null`, 1),
		"duplicate mode":             strings.TrimSuffix(membershipJSON, "}") + `,"mode":"integrated"}`,
		"mode case alias":            strings.Replace(membershipJSON, `"mode"`, `"Mode"`, 1),
		"member case alias":          strings.Replace(membershipJSON, `"id"`, `"ID"`, 1),
		"trailing object":            membershipJSON + `{}`,
		"trailing null":              membershipJSON + `null`,
		"trailing garbage":           membershipJSON + `!`,
		"truncated":                  strings.TrimSuffix(membershipJSON, "}"),
		"wrong id type":              strings.Replace(membershipJSON, `"node1"`, `1`, 1),
		"null member":                `{"mode":"standalone","members":[null,{"id":"node2","url":"http://node2:8080"},{"id":"node3","url":"http://node3:8080"}]}`,
		"empty id":                   strings.Replace(membershipJSON, `"node2"`, `""`, 1),
		"invalid id":                 strings.Replace(membershipJSON, `"node2"`, `"node.2"`, 1),
		"unicode id":                 strings.Replace(membershipJSON, `"node2"`, `"nó2"`, 1),
		"long id":                    strings.Replace(membershipJSON, `"node2"`, `"`+strings.Repeat("a", 65)+`"`, 1),
		"duplicate id":               strings.Replace(membershipJSON, `"node2"`, `"node1"`, 1),
		"duplicate url":              strings.Replace(membershipJSON, "node2:8080", "NODE1:08080/", 1),
		"duplicate absolute DNS URL": strings.Replace(membershipJSON, "node2:8080", "node1.:8080", 1),
		"oversized":                  membershipJSON + strings.Repeat(" ", maxConfigBytes-len(membershipJSON)+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadJSON(t, body, "node1", "run"); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
	for _, id := range []string{"", "missing", "node 1", "nó1", strings.Repeat("a", 65)} {
		if _, err := loadJSON(t, membershipJSON, id, "run"); err == nil {
			t.Fatalf("invalid local identity %q accepted", id)
		}
	}
	for _, run := range []string{"", "run.1", "run 1", "execução", strings.Repeat("a", 65)} {
		if _, err := loadJSON(t, membershipJSON, "node1", run); err == nil {
			t.Fatalf("invalid run identity %q accepted", run)
		}
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.json"), "node1", "run"); err == nil {
		t.Fatal("missing configuration accepted")
	}
}

func TestConfigurationBoundary(t *testing.T) {
	body := strings.Replace(membershipJSON, `"node1"`, `"`+strings.Repeat("a", 64)+`"`, 1)
	body += strings.Repeat(" ", maxConfigBytes-len(body))
	if _, err := loadJSON(t, body, strings.Repeat("a", 64), strings.Repeat("R", 64)); err != nil {
		t.Fatalf("64KiB file or 64-byte identities rejected: %v", err)
	}
}

func TestURLValidation(t *testing.T) {
	for _, raw := range []string{
		"", "node1:8080", "https://node1:8080", "http:node1:8080", "http://node1", "http://node1:",
		"http://:8080", "http://node1:0", "http://node1:65536", "http://node1:-1", "http://node1:+80",
		"http://node1:http", "http://node1:99999999999999999999999999", "http://user:pass@node1:8080",
		"http://@node1:8080", "http://node1:8080/path", "http://node1:8080//", "http://node1:8080/%2f",
		"http://node1:8080?", "http://node1:8080?a=1", "http://node1:8080#", "http://node1:8080#fragment",
		"http://node_1:8080", "http://-node1:8080", "http://node1-:8080", "http://node..one:8080",
		"http://999.1.1.1:8080", "http://127.00.0.1:8080", "http://123:8080", "http://.:8080",
		"http://[node1]:8080", "http://[127.0.0.1]:8080", "http://[:::1]:8080", "http://::1:8080",
		"http://[fe80::1%25eth0]:8080", "http://" + strings.Repeat("a", 64) + ":8080",
		"http://" + strings.Repeat("abc.", 64) + "com:8080",
	} {
		t.Run(raw, func(t *testing.T) {
			body := strings.Replace(membershipJSON, "http://node1:8080", raw, 1)
			if _, err := loadJSON(t, body, "node1", "run"); err == nil {
				t.Fatal("invalid URL accepted")
			}
		})
	}
	for raw, want := range map[string]string{
		"http://LOCALHOST:00001/":            "http://localhost:1",
		"http://127.0.0.1:65535":             "http://127.0.0.1:65535",
		"http://[2001:0DB8:0:0:0:0:0:1]:80/": "http://[2001:db8::1]:80",
		"http://[::ffff:127.0.0.1]:8080":     "http://127.0.0.1:8080",
	} {
		t.Run(raw, func(t *testing.T) {
			body := strings.Replace(membershipJSON, "http://node1:8080", raw, 1)
			cluster, err := loadJSON(t, body, "node1", "run")
			if err != nil || cluster.Members[0].URL != want {
				t.Fatalf("canonical URL: got %+v, error %v; want %q", cluster.Members, err, want)
			}
		})
	}
	body := strings.Replace(membershipJSON, "http://node1:8080", "http://[0:0:0:0:0:0:0:1]:8080", 1)
	body = strings.Replace(body, "http://node2:8080", "http://[::1]:08080/", 1)
	if _, err := loadJSON(t, body, "node1", "run"); err == nil {
		t.Fatal("equivalent IPv6 endpoints accepted as distinct members")
	}
}
