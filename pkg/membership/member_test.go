package membership

import (
	"io"
	"log"
	"os"
	"sort"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	// Production code logs state transitions; silence it for test output.
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

var baseTime = time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC)

func newTestMembership(infos map[uint64]Info) *Membership {
	m := &Membership{
		InfoMap: make(map[uint64]Info, len(infos)),
		Members: make([]uint64, 0, len(infos)),
	}
	for id, info := range infos {
		m.InfoMap[id] = info
		if info.State != Failed {
			m.Members = append(m.Members, id)
		}
	}
	return m
}

func testInfo(host string, port int, counter uint64, state MemberState, ts time.Time) Info {
	return Info{
		Hostname:  host,
		Port:      port,
		Version:   baseTime,
		Timestamp: ts,
		Counter:   counter,
		State:     state,
	}
}

func sortedCopy(ids []uint64) []uint64 {
	out := append([]uint64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func membersEqual(t *testing.T, got []uint64, want []uint64) {
	t.Helper()
	g, w := sortedCopy(got), sortedCopy(want)
	if len(g) != len(w) {
		t.Fatalf("Members = %v, want elements %v", got, want)
	}
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("Members = %v, want elements %v", got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

func TestMergeAddsNewAliveMember(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
	})

	now := baseTime.Add(10 * time.Second)
	incoming := map[uint64]Info{
		2: testInfo("node2", 8787, 3, Alive, baseTime), // stale incoming Timestamp
	}

	if changed := m.Merge(incoming, now); !changed {
		t.Fatal("Merge returned false, want true when adding a new member")
	}
	got, ok := m.InfoMap[2]
	if !ok {
		t.Fatal("new member 2 not added to InfoMap")
	}
	if got.State != Alive || got.Counter != 3 {
		t.Fatalf("new member info = %+v, want Alive with counter 3", got)
	}
	// Incoming Timestamp is replaced with currentTime, not the sender's value.
	if !got.Timestamp.Equal(now) {
		t.Fatalf("new member Timestamp = %v, want currentTime %v", got.Timestamp, now)
	}
	membersEqual(t, m.Members, []uint64{1, 2})
}

func TestMergeIgnoresBrandNewFailedMember(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
	})

	incoming := map[uint64]Info{
		2: testInfo("node2", 8787, 9, Failed, baseTime),
	}
	if changed := m.Merge(incoming, baseTime.Add(time.Second)); changed {
		t.Fatal("Merge returned true, want false for a previously-unknown Failed member")
	}
	if _, ok := m.InfoMap[2]; ok {
		t.Fatal("previously-unknown Failed member was added to InfoMap")
	}
	membersEqual(t, m.Members, []uint64{1})
}

func TestMergeSkipsInvalidInfo(t *testing.T) {
	// The only validation in Merge is Port == 0 or Hostname == "".
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
	})

	incoming := map[uint64]Info{
		2: testInfo("node2", 0, 99, Alive, baseTime), // zero Port
		3: testInfo("", 8787, 99, Alive, baseTime),   // empty Hostname
	}
	if changed := m.Merge(incoming, baseTime.Add(time.Second)); changed {
		t.Fatal("Merge returned true, want false when all incoming entries are invalid")
	}
	if _, ok := m.InfoMap[2]; ok {
		t.Fatal("entry with zero Port was merged")
	}
	if _, ok := m.InfoMap[3]; ok {
		t.Fatal("entry with empty Hostname was merged")
	}
}

func TestMergeHigherCounterWins(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
	})

	now := baseTime.Add(time.Second)
	incoming := map[uint64]Info{
		1: testInfo("node1", 8787, 8, Alive, baseTime),
	}
	// Note: a counter-only refresh does NOT count as a membership change,
	// so Merge returns false even though InfoMap was updated.
	if changed := m.Merge(incoming, now); changed {
		t.Fatal("Merge returned true, want false for a counter-only update")
	}
	got := m.InfoMap[1]
	if got.Counter != 8 {
		t.Fatalf("Counter = %d, want 8 (higher incoming counter wins)", got.Counter)
	}
	if !got.Timestamp.Equal(now) {
		t.Fatalf("Timestamp = %v, want refreshed to currentTime %v", got.Timestamp, now)
	}
}

func TestMergeLowerOrEqualCounterIgnored(t *testing.T) {
	orig := testInfo("node1", 8787, 5, Alive, baseTime)
	for _, counter := range []uint64{4, 5} {
		m := newTestMembership(map[uint64]Info{1: orig})
		incoming := map[uint64]Info{
			1: testInfo("node1", 8787, counter, Alive, baseTime),
		}
		if changed := m.Merge(incoming, baseTime.Add(time.Second)); changed {
			t.Fatalf("Merge returned true for incoming counter %d, want false", counter)
		}
		got := m.InfoMap[1]
		if got.Counter != 5 {
			t.Fatalf("Counter = %d after incoming counter %d, want unchanged 5", got.Counter, counter)
		}
		// Timestamp must not be refreshed for a stale/equal counter.
		if !got.Timestamp.Equal(baseTime) {
			t.Fatalf("Timestamp = %v, want unchanged %v", got.Timestamp, baseTime)
		}
	}
}

func TestMergeMarksExistingMemberFailed(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
		2: testInfo("node2", 8787, 5, Alive, baseTime),
	})

	now := baseTime.Add(time.Second)
	incoming := map[uint64]Info{
		// Counter is lower, but Failed state still wins over a non-Failed local state.
		2: testInfo("node2", 8787, 1, Failed, baseTime),
	}
	if changed := m.Merge(incoming, now); !changed {
		t.Fatal("Merge returned false, want true when an existing member is marked Failed")
	}
	got := m.InfoMap[2]
	if got.State != Failed {
		t.Fatalf("State = %v, want Failed", got.State)
	}
	if !got.Timestamp.Equal(now) {
		t.Fatalf("Timestamp = %v, want currentTime %v", got.Timestamp, now)
	}
	// Members list is rebuilt excluding Failed members.
	membersEqual(t, m.Members, []uint64{1})
}

func TestMergeAliveWithHigherCounterResurrectsFailedInfoButNotMembers(t *testing.T) {
	// Pin surprising behavior: when a locally-Failed member arrives as Alive
	// with a higher counter, the `info.Counter > member.Counter` branch
	// overwrites the Failed state in InfoMap — but memberChanged stays false,
	// so Merge returns false and the Members list is NOT rebuilt. The member
	// is Alive in InfoMap yet absent from Members.
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
		2: testInfo("node2", 8787, 5, Failed, baseTime),
	})

	incoming := map[uint64]Info{
		2: testInfo("node2", 8787, 9, Alive, baseTime),
	}
	if changed := m.Merge(incoming, baseTime.Add(time.Second)); changed {
		t.Fatal("Merge returned true, want false (resurrection is not flagged as a change)")
	}
	got := m.InfoMap[2]
	if got.State != Alive || got.Counter != 9 {
		t.Fatalf("InfoMap[2] = %+v, want Alive with counter 9", got)
	}
	// Still excluded from Members despite being Alive in InfoMap.
	membersEqual(t, m.Members, []uint64{1})
}

// ---------------------------------------------------------------------------
// UpdateStateGossip
// ---------------------------------------------------------------------------

func TestUpdateStateGossipWithSuspicion(t *testing.T) {
	const (
		Tfail    = 10 * time.Second
		Tsuspect = 5 * time.Second
	)
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
		2: testInfo("node2", 8787, 5, Alive, baseTime),
	})

	// Within Tsuspect: nothing happens.
	if failed := m.UpdateStateGossip(baseTime.Add(Tsuspect), Tfail, Tsuspect, true); failed {
		t.Fatal("UpdateStateGossip returned true before any timeout")
	}
	if m.InfoMap[1].State != Alive {
		t.Fatalf("State = %v before Tsuspect elapsed, want Alive", m.InfoMap[1].State)
	}

	// Past Tsuspect: Alive -> Suspected, but the return value is still false
	// (it only reports failures, not suspicions).
	t1 := baseTime.Add(Tsuspect + time.Second)
	if failed := m.UpdateStateGossip(t1, Tfail, Tsuspect, true); failed {
		t.Fatal("UpdateStateGossip returned true for a suspicion-only transition, want false")
	}
	got := m.InfoMap[1]
	if got.State != Suspected {
		t.Fatalf("State = %v after Tsuspect elapsed, want Suspected", got.State)
	}
	if !got.Timestamp.Equal(t1) {
		t.Fatalf("Timestamp = %v after suspicion, want reset to %v", got.Timestamp, t1)
	}
	// Suspicion does not remove members from the Members list.
	membersEqual(t, m.Members, []uint64{1, 2})

	// Past Tfail measured from the suspicion timestamp: Suspected -> Failed,
	// returns true, Members rebuilt excluding Failed.
	t2 := t1.Add(Tfail + time.Second)
	if failed := m.UpdateStateGossip(t2, Tfail, Tsuspect, true); !failed {
		t.Fatal("UpdateStateGossip returned false, want true when a member fails")
	}
	if m.InfoMap[1].State != Failed {
		t.Fatalf("State = %v after Tfail elapsed, want Failed", m.InfoMap[1].State)
	}
	// Note: member 2 also got suspected (and then failed) along the way since
	// its timestamp was never refreshed either; both end up Failed here.
	if m.InfoMap[2].State != Failed {
		t.Fatalf("member 2 State = %v, want Failed", m.InfoMap[2].State)
	}
	membersEqual(t, m.Members, []uint64{})
}

func TestUpdateStateGossipNoSuspicionFailsDirectly(t *testing.T) {
	const (
		Tfail    = 10 * time.Second
		Tsuspect = 5 * time.Second
	)
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
		2: testInfo("node2", 8787, 5, Alive, baseTime.Add(Tfail)), // recent heartbeat
	})

	// Exactly Tfail elapsed is not enough (strict >).
	if failed := m.UpdateStateGossip(baseTime.Add(Tfail), Tfail, Tsuspect, false); failed {
		t.Fatal("UpdateStateGossip returned true at exactly Tfail, want false (strict >)")
	}

	now := baseTime.Add(Tfail + time.Second)
	if failed := m.UpdateStateGossip(now, Tfail, Tsuspect, false); !failed {
		t.Fatal("UpdateStateGossip returned false, want true when a member fails")
	}
	if m.InfoMap[1].State != Failed {
		t.Fatalf("member 1 State = %v, want Failed (Alive -> Failed directly)", m.InfoMap[1].State)
	}
	if m.InfoMap[2].State != Alive {
		t.Fatalf("member 2 State = %v, want still Alive", m.InfoMap[2].State)
	}
	membersEqual(t, m.Members, []uint64{2})
}

// ---------------------------------------------------------------------------
// UpdateStateSwim
// ---------------------------------------------------------------------------

func TestUpdateStateSwimSuspectedNoSuspicionGoesDirectlyToFailed(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
		2: testInfo("node2", 8787, 5, Alive, baseTime),
	})

	now := baseTime.Add(time.Second)
	// With suspicion disabled, a Suspected update is converted to Failed.
	if failed := m.UpdateStateSwim(now, 1, Suspected, false); !failed {
		t.Fatal("UpdateStateSwim returned false, want true (Suspected becomes Failed without suspicion)")
	}
	if m.InfoMap[1].State != Failed {
		t.Fatalf("State = %v, want Failed", m.InfoMap[1].State)
	}
	membersEqual(t, m.Members, []uint64{2})
}

func TestUpdateStateSwimSuspectedWithSuspicionStaysSuspected(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
		2: testInfo("node2", 8787, 5, Alive, baseTime),
	})

	now := baseTime.Add(time.Second)
	if failed := m.UpdateStateSwim(now, 1, Suspected, true); failed {
		t.Fatal("UpdateStateSwim returned true for a suspicion, want false")
	}
	got := m.InfoMap[1]
	if got.State != Suspected {
		t.Fatalf("State = %v, want Suspected", got.State)
	}
	if !got.Timestamp.Equal(now) {
		t.Fatalf("Timestamp = %v, want %v", got.Timestamp, now)
	}
	// Suspected members remain in the Members list.
	membersEqual(t, m.Members, []uint64{1, 2})
}

func TestUpdateStateSwimIgnoresFailedAndUnknownMembers(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Failed, baseTime),
	})

	now := baseTime.Add(time.Second)
	// Already-Failed members are never updated (even back to Alive).
	if failed := m.UpdateStateSwim(now, 1, Alive, true); failed {
		t.Fatal("UpdateStateSwim returned true for an already-Failed member, want false")
	}
	if m.InfoMap[1].State != Failed {
		t.Fatalf("State = %v, want still Failed", m.InfoMap[1].State)
	}
	// Unknown id is a no-op returning false.
	if failed := m.UpdateStateSwim(now, 999, Failed, true); failed {
		t.Fatal("UpdateStateSwim returned true for an unknown id, want false")
	}
}

// ---------------------------------------------------------------------------
// Cleanup
// ---------------------------------------------------------------------------

func TestCleanupRemovesOldFailedKeepsRecentAndAlive(t *testing.T) {
	const Tcleanup = 30 * time.Second
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),                      // old but Alive
		2: testInfo("node2", 8787, 5, Failed, baseTime),                     // Failed past the window
		3: testInfo("node3", 8787, 5, Failed, baseTime.Add(40*time.Second)), // Failed, recent
		4: testInfo("node4", 8787, 5, Suspected, baseTime),                  // Suspected never removed
	})

	now := baseTime.Add(Tcleanup + time.Second)
	m.Cleanup(now, Tcleanup)

	if _, ok := m.InfoMap[2]; ok {
		t.Fatal("Failed member past Tcleanup was not removed")
	}
	if _, ok := m.InfoMap[1]; !ok {
		t.Fatal("Alive member was removed by Cleanup")
	}
	if _, ok := m.InfoMap[3]; !ok {
		t.Fatal("recently Failed member (within Tcleanup) was removed")
	}
	if _, ok := m.InfoMap[4]; !ok {
		t.Fatal("Suspected member was removed by Cleanup")
	}
}

// ---------------------------------------------------------------------------
// Heartbeat
// ---------------------------------------------------------------------------

func TestHeartbeatIncrementsCounterAndRefreshesTimestamp(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
	})

	now := baseTime.Add(time.Second)
	if err := m.Heartbeat(1, now); err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	got := m.InfoMap[1]
	if got.Counter != 6 {
		t.Fatalf("Counter = %d, want 6", got.Counter)
	}
	if !got.Timestamp.Equal(now) {
		t.Fatalf("Timestamp = %v, want %v", got.Timestamp, now)
	}
}

func TestHeartbeatErrors(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Failed, baseTime),
	})

	now := baseTime.Add(time.Second)
	// Heartbeat on a Failed self returns the "you failed" error and does not bump the counter.
	err := m.Heartbeat(1, now)
	if err == nil {
		t.Fatal("Heartbeat on a Failed member returned nil, want error")
	}
	if err.Error() != "you failed" {
		t.Fatalf("error = %q, want %q", err.Error(), "you failed")
	}
	if m.InfoMap[1].Counter != 5 {
		t.Fatalf("Counter = %d, want unchanged 5", m.InfoMap[1].Counter)
	}

	// Unknown id returns an error.
	if err := m.Heartbeat(999, now); err == nil {
		t.Fatal("Heartbeat on an unknown id returned nil, want error")
	}
}

// ---------------------------------------------------------------------------
// RemoveMember
// ---------------------------------------------------------------------------

func TestRemoveMember(t *testing.T) {
	m := newTestMembership(map[uint64]Info{
		1: testInfo("node1", 8787, 5, Alive, baseTime),
		2: testInfo("node2", 8787, 5, Alive, baseTime),
		3: testInfo("node3", 8787, 5, Alive, baseTime),
	})

	m.RemoveMember(2)

	if _, ok := m.InfoMap[2]; ok {
		t.Fatal("removed member still present in InfoMap")
	}
	membersEqual(t, m.Members, []uint64{1, 3})

	// Removing an unknown id is a no-op.
	m.RemoveMember(999)
	if len(m.InfoMap) != 2 {
		t.Fatalf("InfoMap size = %d after removing unknown id, want 2", len(m.InfoMap))
	}
	membersEqual(t, m.Members, []uint64{1, 3})
}

// ---------------------------------------------------------------------------
// Hash / permutation utils
// ---------------------------------------------------------------------------

func TestHashInfoDeterministic(t *testing.T) {
	a := testInfo("node1", 8787, 5, Alive, baseTime)
	b := testInfo("node1", 8787, 99, Failed, baseTime.Add(time.Hour)) // Counter/State/Timestamp not hashed

	first, second := HashInfo(a), HashInfo(a)
	if first != second {
		t.Fatal("HashInfo is not deterministic for identical Info")
	}
	// Hash covers only Hostname, Port, and Version.
	if HashInfo(a) != HashInfo(b) {
		t.Fatal("HashInfo should ignore Counter, State, and Timestamp")
	}

	c := testInfo("node2", 8787, 5, Alive, baseTime)
	if HashInfo(a) == HashInfo(c) {
		t.Fatal("HashInfo collided for different hostnames")
	}
	d := a
	d.Port = 9999
	if HashInfo(a) == HashInfo(d) {
		t.Fatal("HashInfo collided for different ports")
	}
	e := a
	e.Version = baseTime.Add(time.Second)
	if HashInfo(a) == HashInfo(e) {
		t.Fatal("HashInfo collided for different versions")
	}
}

func TestRandomPermutationPreservesElements(t *testing.T) {
	// The permutation is seeded from time.Now(), so order is unspecified;
	// only assert the multiset of elements is preserved.
	orig := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	arr := append([]uint64(nil), orig...)
	RandomPermutation(&arr)

	if len(arr) != len(orig) {
		t.Fatalf("length = %d after permutation, want %d", len(arr), len(orig))
	}
	membersEqual(t, arr, orig)

	// Empty and single-element slices must not panic.
	empty := []uint64{}
	RandomPermutation(&empty)
	single := []uint64{42}
	RandomPermutation(&single)
	if single[0] != 42 {
		t.Fatalf("single-element permutation = %v, want [42]", single)
	}
}
