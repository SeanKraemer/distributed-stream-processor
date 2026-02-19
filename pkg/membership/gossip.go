package membership

import (
	"log"
	"os"
	"time"
)

type Gossip struct {
	Membership *Membership // membership
}

func (g *Gossip) HandleIncomingMessage(message Message) {
	// merge incoming membership info
	anyChanged := g.Membership.Merge(message.InfoMap, time.Now())
	if anyChanged {
		log.Printf("Membership updated: %s\n", g.Membership.String())
	}
}

func (g *Gossip) GossipStep(
	myId uint64,
	Tfail time.Duration,
	Tsuspect time.Duration,
	Tcleanup time.Duration,
	suspicionEnabled bool) int64 {
	currentTime := time.Now()

	// increase heartbeat counter
	err := g.Membership.Heartbeat(myId, currentTime)
	if err != nil {
		log.Fatalf("Failed to heartbeat: %s", err.Error())
	}

	// update state
	g.Membership.UpdateStateGossip(currentTime, Tfail, Tsuspect, suspicionEnabled)

	// cleanup
	g.Membership.Cleanup(currentTime, Tcleanup)

	// get gossip target info
	targetInfo, err := g.Membership.GetTarget()
	if err != nil {
		log.Printf("Failed to get target info: %s", err.Error())
		os.Exit(1) // auto restart
	} else {
		// copy member info map to send
		infoMap := g.Membership.GetInfoMap()

		// Send Gossip
		message := Message{
			Type:    GossipMsg,
			InfoMap: infoMap,
		}
		n, err := SendMessage(message, targetInfo.Hostname, targetInfo.Port)
		if err != nil {
			log.Printf("failed to send gossip message to %s:%d: %s", targetInfo.Hostname, targetInfo.Port, err.Error())
		} else {
			return n
		}
	}
	return 0
}

func NewGossip(membership *Membership) *Gossip {
	return &Gossip{
		Membership: membership,
	}
}
