package historyserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// TestSessionHeapReport prints, per entity type, how much live Go heap a decoded
// SessionSnapshot uses compared to the JSON length the LRU cache charges for it
// (see putSnapshot: size = len(json.Marshal(snap))). Run with:
//
//	go test ./pkg/historyserver -run TestSessionHeapReport -v
//
// Entity shapes come from real Ray events in testdata/ray_events (job/node
// events dumped from a MinIO bucket written by the collector, log events from
// a local `ray job submit`) replayed through the real ingestion path, then
// cloned round-robin with unique IDs up to n entries.
//
// It only prints a report, so it is skipped under -short to keep CI quiet.
func TestSessionHeapReport(t *testing.T) {
	if testing.Short() {
		t.Skip("report only")
	}
	const n = 10_000
	seed := replaySession(t)

	rows := []struct {
		name  string
		build func() *eventserver.SessionSnapshot
	}{
		{"Tasks", func() *eventserver.SessionSnapshot { return tasksOnly(seed, n) }},
		{"Actors", func() *eventserver.SessionSnapshot { return actorsOnly(seed, n) }},
		{"Jobs", func() *eventserver.SessionSnapshot { return jobsOnly(seed, n) }},
		{"Nodes", func() *eventserver.SessionSnapshot { return nodesOnly(seed, n) }},
		{"LogEvents", func() *eventserver.SessionSnapshot { return logEventsOnly(seed, n) }},
	}

	var b strings.Builder
	fmt.Fprintf(&b, "\n| Type | Entries | JSON | Live heap | JSON/entry | Heap/entry | Ratio |\n|---|---:|---:|---:|---:|---:|---:|\n")
	for _, row := range rows {
		// Build and encode in one statement so the fixture is garbage before liveHeap measures.
		encoded, err := json.Marshal(row.build())
		if err != nil {
			t.Fatal(err)
		}
		jsonBytes, heapBytes := len(encoded), liveHeap(t, encoded)
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %d B | %d B | %.2fx |\n",
			row.name, n, mb(jsonBytes), mb(heapBytes), jsonBytes/n, heapBytes/n, float64(heapBytes)/float64(jsonBytes))
	}
	t.Log(b.String())
}

// The *Only builders return a snapshot holding n entries of a single type,
// cycling through the seed entries and stamping each with a unique ID at
// Ray's real ID width.

func tasksOnly(seed *eventserver.SessionSnapshot, n int) *eventserver.SessionSnapshot {
	snap := &eventserver.SessionSnapshot{}
	for i := range n {
		tk := seed.Tasks[i%len(seed.Tasks)]
		tk.TaskID = fmt.Sprintf("%040x%08d", i, i)
		snap.Tasks = append(snap.Tasks, tk)
	}
	return snap
}

func actorsOnly(seed *eventserver.SessionSnapshot, n int) *eventserver.SessionSnapshot {
	pool := slices.Collect(maps.Values(seed.Actors))
	snap := &eventserver.SessionSnapshot{Actors: map[string]eventtypes.Actor{}}
	for i := range n {
		a := pool[i%len(pool)]
		a.ActorID = fmt.Sprintf("%024x%08d", i, i)
		snap.Actors[a.ActorID] = a
	}
	return snap
}

func jobsOnly(seed *eventserver.SessionSnapshot, n int) *eventserver.SessionSnapshot {
	pool := slices.Collect(maps.Values(seed.Jobs))
	snap := &eventserver.SessionSnapshot{Jobs: map[string]eventtypes.Job{}}
	for i := range n {
		j := pool[i%len(pool)]
		j.JobID = fmt.Sprintf("%08x", i)
		snap.Jobs[j.JobID] = j
	}
	return snap
}

func nodesOnly(seed *eventserver.SessionSnapshot, n int) *eventserver.SessionSnapshot {
	pool := slices.Collect(maps.Values(seed.Nodes))
	snap := &eventserver.SessionSnapshot{Nodes: map[string]eventtypes.Node{}}
	for i := range n {
		nd := pool[i%len(pool)]
		nd.NodeID = fmt.Sprintf("%048x%08d", i, i)
		snap.Nodes[nd.NodeID] = nd
	}
	return snap
}

func logEventsOnly(seed *eventserver.SessionSnapshot, n int) *eventserver.SessionSnapshot {
	pool := slices.Concat(slices.Collect(maps.Values(seed.LogEventsByJobID))...)
	events := make([]eventtypes.LogEvent, 0, n)
	for i := range n {
		e := pool[i%len(pool)]
		e.EventID = fmt.Sprintf("%036X", i)
		events = append(events, e)
	}
	return &eventserver.SessionSnapshot{LogEventsByJobID: map[string][]eventtypes.LogEvent{"01000000": events}}
}

// replaySession feeds testdata/ray_events through the real ingestion path
// (ProcessSingleSession + BuildSnapshot), exactly what a cold load does.
func replaySession(t *testing.T) *eventserver.SessionSnapshot {
	t.Helper()
	info := utils.ClusterInfo{Name: "replay", Namespace: "default", SessionName: "session_replay"}
	h := eventserver.NewEventHandler(dirStorageReader("testdata/ray_events"))
	if err := h.ProcessSingleSession(context.Background(), info); err != nil {
		t.Fatal(err)
	}
	snap := h.BuildSnapshot(info)
	if len(snap.Tasks) == 0 || len(snap.Actors) == 0 || len(snap.Jobs) == 0 || len(snap.Nodes) == 0 || len(snap.LogEventsByJobID) == 0 {
		t.Fatalf("replay produced an incomplete snapshot: %d tasks, %d actors, %d jobs, %d nodes, %d log event jobs",
			len(snap.Tasks), len(snap.Actors), len(snap.Jobs), len(snap.Nodes), len(snap.LogEventsByJobID))
	}
	return snap
}

// dirStorageReader serves a local directory laid out like the collector's
// storage bucket below the cluster prefix:
// <session>/<nodeId>/{job_events,node_events,logs/events}/...
type dirStorageReader string

func (dirStorageReader) List() []utils.ClusterInfo { return nil }

func (d dirStorageReader) GetContent(_ string, fileName string) io.Reader {
	f, err := os.Open(filepath.Join(string(d), fileName))
	if err != nil {
		return nil
	}
	return f
}

// ListFiles marks directories with a trailing slash, matching what the real readers do.
func (d dirStorageReader) ListFiles(_ string, dir string) []string {
	entries, err := os.ReadDir(filepath.Join(string(d), dir))
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name()+"/")
		} else {
			names = append(names, e.Name())
		}
	}
	return names
}

// liveHeap decodes encoded (the same path a cold load takes) and returns the
// heap the decoded object graph occupies after GC. encoded itself is live for
// both readings, so it cancels out.
func liveHeap(t *testing.T, encoded []byte) int {
	t.Helper()
	var before, after runtime.MemStats
	// Twice: json.Marshal parks its encode buffer in a sync.Pool, which
	// survives one GC (victim cache) and is freed on the next.
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)

	decoded := new(eventserver.SessionSnapshot)
	if err := json.Unmarshal(encoded, decoded); err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(decoded)
	runtime.KeepAlive(encoded)
	return int(after.HeapAlloc - before.HeapAlloc)
}

func mb(b int) string { return fmt.Sprintf("%.2f MB", float64(b)/(1<<20)) }
