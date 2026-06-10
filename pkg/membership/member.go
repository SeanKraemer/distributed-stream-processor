package membership

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

// member states
type MemberState int

const (
	Alive MemberState = iota
	Failed
	Suspected
)

var stateName = map[MemberState]string{
	Alive:     "Alive",
	Failed:    "Failed",
	Suspected: "Suspected",
}

type Info struct {
	Hostname  string      // hostname
	Port      int         // port number
	Version   time.Time   // version timestamp
	Timestamp time.Time   // local timestamp
	Counter   uint64      // heartbeat counter
	State     MemberState // member state
}

type Membership struct {
	lock            sync.RWMutex    // read write mutex
	Members         []uint64        // list of member Ids that is randomly permuted for gossip or pinging
	InfoMap         map[uint64]Info // member info map (might contain info of failed members)
	roundRobinIndex int             // random permutation round robin index
}

func (i *Info) String() string {
	return fmt.Sprintf("Hostname: %s, Port: %d, Version: %s", i.Hostname, i.Port, i.Version.Format(time.RFC3339Nano))
}

func (m *Membership) Merge(memberInfo map[uint64]Info, currentTime time.Time) bool {
	// merge memberInfo into m
	m.lock.Lock()
	defer m.lock.Unlock()
	memberChanged := false
	for id, info := range memberInfo {
		if info.Port == 0 || info.Hostname == "" {
			continue
		}
		if member, ok := m.InfoMap[id]; ok {
			if info.State == Failed {
				if member.State != Failed {
					info.Timestamp = currentTime
					m.InfoMap[id] = info // update to failed state
					log.Printf("FAILED: Node %d (%s:%d) failed\n",
						id, info.Hostname, info.Port)
					memberChanged = true
				}
			} else if info.Counter > member.Counter {
				// update member info with higher counter
				info.Timestamp = currentTime
				m.InfoMap[id] = info
			}
		} else if info.State != Failed {
			info.Timestamp = currentTime
			m.InfoMap[id] = info // add new member
			memberChanged = true
		}
	}
	if memberChanged {
		m.Members = make([]uint64, 0, len(m.InfoMap))
		for id, info := range m.InfoMap {
			if info.State != Failed {
				m.Members = append(m.Members, id)
			}
		}
		RandomPermutation(&m.Members) // randomize member ids for gossip or pinging
		m.roundRobinIndex = 0         // reset round robin index
	}
	return memberChanged
}

func (m *Membership) UpdateStateGossip(currentTime time.Time, Tfail time.Duration, Tsuspect time.Duration, suspicionEnabled bool) bool {
	// check member states based on timestamps and thresholds
	m.lock.Lock()
	defer m.lock.Unlock()
	anyFailed := false
	for id, info := range m.InfoMap {
		elapsed := currentTime.Sub(info.Timestamp)
		oldState := info.State

		if suspicionEnabled {
			// With suspicion: Alive -> (Tsuspect) -> Suspected -> (Tfail) -> Failed
			if oldState == Alive && elapsed > Tsuspect {
				info.State = Suspected
				info.Timestamp = currentTime
				m.InfoMap[id] = info
				log.Printf("SUSPECTED: Node %d (%s:%d) is now suspected\n",
					id, info.Hostname, info.Port)
			} else if oldState == Suspected && elapsed > Tfail {
				log.Printf("FAILED: Node %d (%s:%d) failed\n",
					id, info.Hostname, info.Port)
				info.State = Failed
				info.Timestamp = currentTime
				m.InfoMap[id] = info
				anyFailed = true
			}
		} else {
			// Without suspicion: Alive -> (Tfail) -> Failed
			if oldState == Alive && elapsed > Tfail {
				log.Printf("FAILED: Node %d (%s:%d) failed (timeout: %v)\n",
					id, info.Hostname, info.Port, elapsed)
				info.State = Failed
				info.Timestamp = currentTime
				m.InfoMap[id] = info
				anyFailed = true
			}
		}
	}
	if anyFailed {
		m.Members = make([]uint64, 0, len(m.InfoMap))
		for id, info := range m.InfoMap {
			if info.State != Failed {
				m.Members = append(m.Members, id)
			}
		}
		RandomPermutation(&m.Members) // randomize member ids for gossip or pinging
		m.roundRobinIndex = 0         // reset round robin index
	}
	return anyFailed
}

func (m *Membership) UpdateStateSwim(currentTime time.Time, id uint64, state MemberState, suspicionEnabled bool) bool {
	// update state for a specific id, return true if any member beceom Failed.
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.InfoMap[id]
	if ok && info.State != Failed {
		oldState := info.State
		info.Timestamp = currentTime

		if state == Suspected && !suspicionEnabled {
			// Skip suspicion, go directly to failed
			info.State = Failed
		} else {
			info.State = state
		}
		m.InfoMap[id] = info

		if info.State == Suspected && oldState != Suspected && suspicionEnabled {
			log.Printf("SUSPECTED: Node %d (%s:%d) is now suspected\n",
				id, info.Hostname, info.Port)
		}

		if info.State == Failed {
			log.Printf("FAILED: Node %d (%s:%d) failed\n",
				id, info.Hostname, info.Port)
			m.Members = make([]uint64, 0, len(m.InfoMap))
			for id, info := range m.InfoMap {
				if info.State != Failed {
					m.Members = append(m.Members, id)
				}
			}
			RandomPermutation(&m.Members) // randomize member ids for gossip or pinging
			m.roundRobinIndex = 0         // reset round robin index
			return true
		}
	}
	return false
}

func (m *Membership) Cleanup(currentTime time.Time, Tcleanup time.Duration) {
	// remove failed members
	m.lock.Lock()
	defer m.lock.Unlock()
	for id, info := range m.InfoMap {
		if info.State == Failed && currentTime.Sub(info.Timestamp) > Tcleanup {
			delete(m.InfoMap, id) // remove info about failed members
		}
	}
}

func (m *Membership) Reset(currentTime time.Time, hostname string, port int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	newInfo := Info{
		Hostname:  hostname,
		Port:      port,
		Version:   currentTime,
		Timestamp: currentTime,
		Counter:   0,
		State:     Alive,
	}
	newId := HashInfo(newInfo)
	m.Members = []uint64{newId}
	m.InfoMap = map[uint64]Info{
		newId: newInfo,
	}
	m.roundRobinIndex = 0
}

func (m *Membership) RemoveMember(id uint64) {
	// remove a member completely (for voluntary leave)
	m.lock.Lock()
	defer m.lock.Unlock()

	// Remove from InfoMap
	delete(m.InfoMap, id)

	// Remove from Members list
	for i, memberId := range m.Members {
		if memberId == id {
			m.Members = append(m.Members[:i], m.Members[i+1:]...)
			break
		}
	}

	// Reset round robin index if needed
	if m.roundRobinIndex >= len(m.Members) {
		RandomPermutation(&m.Members)
		m.roundRobinIndex = 0
	}
}

func (m *Membership) String() string {
	res := "Membership:\n"
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, id := range m.Members {
		info := m.InfoMap[id]
		res += fmt.Sprintf("ID: %d, Hostname: %s, Port: %d, Version: %s, Timestamp: %s, Counter: %d, State: %s\n",
			id, info.Hostname, info.Port, info.Version.Format(time.RFC3339Nano), info.Timestamp.Format(time.RFC3339Nano), info.Counter, stateName[info.State])
	}
	return res
}

func (m *Membership) Table() string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	type Pair struct {
		Id       uint64
		Hostname string
		Port     int
	}
	infoList := make([]Pair, 0, 16)
	for _, id := range m.Members {
		info := m.InfoMap[id]
		infoList = append(infoList, Pair{
			Id:       id,
			Hostname: info.Hostname,
			Port:     info.Port,
		})
	}
	sort.Slice(infoList, func(i, j int) bool {
		if infoList[i].Hostname != infoList[j].Hostname {
			return infoList[i].Hostname < infoList[j].Hostname
		}
		return infoList[i].Port < infoList[j].Port
	})
	// --------------------------------------------------------------------
	// | ID    |  Hostname | Port | Version | Timestamp | Counter | State |
	// | ID    |  Hostname | Port | Version | Timestamp | Counter | State |
	// --------------------------------------------------------------------
	maxLengths := map[string]int{
		"Id":        2,
		"Hostname":  8,
		"Port":      4,
		"Version":   7,
		"Timestamp": 9,
		"Counter":   7,
		"State":     5,
	}
	for _, i := range infoList {
		info := m.InfoMap[i.Id]
		lengths := map[string]int{
			"Id":        len(fmt.Sprintf("%d", i.Id)),
			"Hostname":  len(i.Hostname),
			"Port":      len(fmt.Sprintf("%d", i.Port)),
			"Version":   len(info.Version.Format(time.RFC3339Nano)),
			"Timestamp": len(info.Timestamp.Format(time.RFC3339Nano)),
			"Counter":   len(fmt.Sprintf("%d", info.Counter)),
			"State":     len(stateName[info.State]),
		}
		for key, value := range lengths {
			if maxLengths[key] < value {
				maxLengths[key] = value
			}
		}
	}
	totalLength := 22
	for _, v := range maxLengths {
		totalLength = totalLength + v
	}

	res := strings.Repeat("-", totalLength) + "\n"

	// Add column headers
	header := "| "

	// ID header
	s := "ID"
	if len(s) < maxLengths["Id"] {
		s = s + strings.Repeat(" ", maxLengths["Id"]-len(s))
	}
	header = header + s + " | "

	// Hostname header
	s = "Hostname"
	if len(s) < maxLengths["Hostname"] {
		s = s + strings.Repeat(" ", maxLengths["Hostname"]-len(s))
	}
	header = header + s + " | "

	// Port header
	s = "Port"
	if len(s) < maxLengths["Port"] {
		s = s + strings.Repeat(" ", maxLengths["Port"]-len(s))
	}
	header = header + s + " | "

	// Version header
	s = "Version"
	if len(s) < maxLengths["Version"] {
		s = s + strings.Repeat(" ", maxLengths["Version"]-len(s))
	}
	header = header + s + " | "

	// Timestamp header
	s = "Timestamp"
	if len(s) < maxLengths["Timestamp"] {
		s = s + strings.Repeat(" ", maxLengths["Timestamp"]-len(s))
	}
	header = header + s + " | "

	// Counter header
	s = "Counter"
	if len(s) < maxLengths["Counter"] {
		s = s + strings.Repeat(" ", maxLengths["Counter"]-len(s))
	}
	header = header + s + " | "

	// State header
	s = "State"
	if len(s) < maxLengths["State"] {
		s = s + strings.Repeat(" ", maxLengths["State"]-len(s))
	}
	header = header + s + " |"

	res = res + header + "\n"
	res = res + strings.Repeat("-", totalLength) + "\n"

	for _, i := range infoList {
		info := m.InfoMap[i.Id]
		line := "| "

		// Id
		s := fmt.Sprintf("%d", i.Id)
		if len(s) < maxLengths["Id"] {
			s = s + strings.Repeat(" ", maxLengths["Id"]-len(s))
		}
		line = line + s + " | "

		// Hostname
		s = i.Hostname
		if len(s) < maxLengths["Hostname"] {
			s = s + strings.Repeat(" ", maxLengths["Hostname"]-len(s))
		}
		line = line + s + " | "

		// Port
		s = fmt.Sprintf("%d", i.Port)
		if len(s) < maxLengths["Port"] {
			s = s + strings.Repeat(" ", maxLengths["Port"]-len(s))
		}
		line = line + s + " | "

		// Version
		s = info.Version.Format(time.RFC3339Nano)
		if len(s) < maxLengths["Version"] {
			s = s + strings.Repeat(" ", maxLengths["Version"]-len(s))
		}
		line = line + s + " | "

		// Timestamp
		s = info.Timestamp.Format(time.RFC3339Nano)
		if len(s) < maxLengths["Timestamp"] {
			s = s + strings.Repeat(" ", maxLengths["Timestamp"]-len(s))
		}
		line = line + s + " | "

		// Counter
		s = fmt.Sprintf("%d", info.Counter)
		if len(s) < maxLengths["Counter"] {
			s = s + strings.Repeat(" ", maxLengths["Counter"]-len(s))
		}
		line = line + s + " | "

		// State
		s = stateName[info.State]
		if len(s) < maxLengths["State"] {
			s = s + strings.Repeat(" ", maxLengths["State"]-len(s))
		}
		line = line + s + " |"

		res = res + line + "\n"
	}
	res = res + strings.Repeat("-", totalLength) + "\n"
	return res
}

func (m *Membership) WholeTable() string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	type Pair struct {
		Id       uint64
		Hostname string
		Port     int
	}
	infoList := make([]Pair, 0, 16)
	for id, info := range m.InfoMap {
		infoList = append(infoList, Pair{
			Id:       id,
			Hostname: info.Hostname,
			Port:     info.Port,
		})
	}
	sort.Slice(infoList, func(i, j int) bool {
		if infoList[i].Hostname != infoList[j].Hostname {
			return infoList[i].Hostname < infoList[j].Hostname
		}
		return infoList[i].Port < infoList[j].Port
	})
	// --------------------------------------------------------------------
	// | ID    |  Hostname | Port | Version | Timestamp | Counter | State |
	// | ID    |  Hostname | Port | Version | Timestamp | Counter | State |
	// --------------------------------------------------------------------
	maxLengths := map[string]int{
		"Id":        2,
		"Hostname":  8,
		"Port":      4,
		"Version":   7,
		"Timestamp": 9,
		"Counter":   7,
		"State":     5,
	}
	for _, i := range infoList {
		info := m.InfoMap[i.Id]
		lengths := map[string]int{
			"Id":        len(fmt.Sprintf("%d", i.Id)),
			"Hostname":  len(i.Hostname),
			"Port":      len(fmt.Sprintf("%d", i.Port)),
			"Version":   len(info.Version.Format(time.RFC3339Nano)),
			"Timestamp": len(info.Timestamp.Format(time.RFC3339Nano)),
			"Counter":   len(fmt.Sprintf("%d", info.Counter)),
			"State":     len(stateName[info.State]),
		}
		for key, value := range lengths {
			if maxLengths[key] < value {
				maxLengths[key] = value
			}
		}
	}
	totalLength := 22
	for _, v := range maxLengths {
		totalLength = totalLength + v
	}

	res := strings.Repeat("-", totalLength) + "\n"

	// Add column headers
	header := "| "

	// ID header
	s := "ID"
	if len(s) < maxLengths["Id"] {
		s = s + strings.Repeat(" ", maxLengths["Id"]-len(s))
	}
	header = header + s + " | "

	// Hostname header
	s = "Hostname"
	if len(s) < maxLengths["Hostname"] {
		s = s + strings.Repeat(" ", maxLengths["Hostname"]-len(s))
	}
	header = header + s + " | "

	// Port header
	s = "Port"
	if len(s) < maxLengths["Port"] {
		s = s + strings.Repeat(" ", maxLengths["Port"]-len(s))
	}
	header = header + s + " | "

	// Version header
	s = "Version"
	if len(s) < maxLengths["Version"] {
		s = s + strings.Repeat(" ", maxLengths["Version"]-len(s))
	}
	header = header + s + " | "

	// Timestamp header
	s = "Timestamp"
	if len(s) < maxLengths["Timestamp"] {
		s = s + strings.Repeat(" ", maxLengths["Timestamp"]-len(s))
	}
	header = header + s + " | "

	// Counter header
	s = "Counter"
	if len(s) < maxLengths["Counter"] {
		s = s + strings.Repeat(" ", maxLengths["Counter"]-len(s))
	}
	header = header + s + " | "

	// State header
	s = "State"
	if len(s) < maxLengths["State"] {
		s = s + strings.Repeat(" ", maxLengths["State"]-len(s))
	}
	header = header + s + " |"

	res = res + header + "\n"
	res = res + strings.Repeat("-", totalLength) + "\n"

	for _, i := range infoList {
		info := m.InfoMap[i.Id]
		line := "| "

		// Id
		s := fmt.Sprintf("%d", i.Id)
		if len(s) < maxLengths["Id"] {
			s = s + strings.Repeat(" ", maxLengths["Id"]-len(s))
		}
		line = line + s + " | "

		// Hostname
		s = i.Hostname
		if len(s) < maxLengths["Hostname"] {
			s = s + strings.Repeat(" ", maxLengths["Hostname"]-len(s))
		}
		line = line + s + " | "

		// Port
		s = fmt.Sprintf("%d", i.Port)
		if len(s) < maxLengths["Port"] {
			s = s + strings.Repeat(" ", maxLengths["Port"]-len(s))
		}
		line = line + s + " | "

		// Version
		s = info.Version.Format(time.RFC3339Nano)
		if len(s) < maxLengths["Version"] {
			s = s + strings.Repeat(" ", maxLengths["Version"]-len(s))
		}
		line = line + s + " | "

		// Timestamp
		s = info.Timestamp.Format(time.RFC3339Nano)
		if len(s) < maxLengths["Timestamp"] {
			s = s + strings.Repeat(" ", maxLengths["Timestamp"]-len(s))
		}
		line = line + s + " | "

		// Counter
		s = fmt.Sprintf("%d", info.Counter)
		if len(s) < maxLengths["Counter"] {
			s = s + strings.Repeat(" ", maxLengths["Counter"]-len(s))
		}
		line = line + s + " | "

		// State
		s = stateName[info.State]
		if len(s) < maxLengths["State"] {
			s = s + strings.Repeat(" ", maxLengths["State"]-len(s))
		}
		line = line + s + " |"

		res = res + line + "\n"
	}
	res = res + strings.Repeat("-", totalLength) + "\n"
	return res
}

func (m *Membership) GetTarget() (Info, error) {
	// Round robin over a random permutation. Self is intentionally not
	// excluded: a self-ping costs one wasted protocol period per cycle and is
	// answered locally, which is harmless and keeps this hot path simple.
	m.lock.Lock()
	defer m.lock.Unlock()
	if len(m.Members) == 0 {
		return Info{}, fmt.Errorf("no existing member")
	}
	targetInfo, ok := m.InfoMap[m.Members[m.roundRobinIndex]]
	if !ok {
		return Info{}, fmt.Errorf("inconsistent membership")
	}
	m.roundRobinIndex++
	if m.roundRobinIndex == len(m.Members) {
		RandomPermutation(&m.Members)
		m.roundRobinIndex = 0
	}
	return targetInfo, nil
}

func (m *Membership) Exists(hostname string, port int) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()
	for _, info := range m.InfoMap {
		if info.Hostname == hostname && info.Port == port {
			return true
		}
	}
	return false
}

func (m *Membership) GetInfoMap() map[uint64]Info {
	m.lock.RLock()
	defer m.lock.RUnlock()

	// Create a new map to hold the copy.
	infoMapCopy := make(map[uint64]Info, len(m.InfoMap))

	// Copy the data from the internal map to the new one.
	for id, info := range m.InfoMap {
		infoMapCopy[id] = info
	}

	return infoMapCopy // Return the safe copy
}

func (m *Membership) Heartbeat(id uint64, currentTime time.Time) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.InfoMap[id]
	if ok {
		if info.State == Failed {
			return fmt.Errorf("you failed")
		}
		info.Timestamp = currentTime
		info.Counter++
		m.InfoMap[id] = info
		return nil
	}
	return fmt.Errorf("no such ID: %d, the node might fail", id)
}

func HashInfo(info Info) uint64 {
	// hash hostname, port, and timestamp to 64 bit integer for map lookup
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s", info.Hostname, info.Port, info.Version.Format(time.RFC3339Nano))))
	return uint64(binary.BigEndian.Uint64(hash[:8]))
}

func RandomPermutation(arr *[]uint64) {
	rng := rand.NewSource(time.Now().UnixNano())
	n := len(*arr)
	for i := 0; i < n; i++ {
		j := rng.Int63() % int64(i+1)
		if int64(i) != j {
			(*arr)[i], (*arr)[j] = (*arr)[j], (*arr)[i] // swap elements
		}
	}
}
