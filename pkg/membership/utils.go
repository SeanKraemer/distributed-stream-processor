package membership

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"
)

// server hostnames — default list used when config is not available.
// In practice these are loaded from config.json.
var HOSTS = []string{
	"node1",
	"node2",
	"node3",
	"node4",
	"node5",
	"node6",
	"node7",
	"node8",
	"node9",
	"node10",
}

const DEFAULT_PORT int = 8787

func GetHostName() (string, error) {
	name, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return name, nil
}

// Define message transmission tools and datatypes
type MessageType int

const (
	Ping MessageType = iota
	Pong
	PingReq
	GossipMsg
	Probe // message for joining
	ProbeAckGossip
	ProbeAckSwim
	UseSwimSus
	UseSwimNoSus
	UseGossipSus
	UseGossipNoSus
	Leave // message for voluntary leave
)

// Message data type for transmission
type Message struct {
	Type          MessageType     // message type
	SenderInfo    Info            // sender's info (counter and timestamp here are not used!!!)
	TargetInfo    Info            // target's info (counter and timestamp here are not used!!!)
	RequesterInfo Info            // requester's info (if direct ping -> sender, if indirect ping -> who start the ping request)
	InfoMap       map[uint64]Info // membership Info map
}

func Serialize(obj Message) ([]byte, error) {
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(obj)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func Deserialize(data []byte) (Message, error) {
	buffer := bytes.Buffer{}
	buffer.Write(data)
	decoder := gob.NewDecoder(&buffer)

	result := Message{}
	err := decoder.Decode(&result)
	if err != nil {
		return Message{}, err
	}
	return result, nil
}

func SendMessage(message Message, hostname string, port int) (int64, error) {
	address := fmt.Sprintf("%s:%d", hostname, port)
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return 0, fmt.Errorf("error resolving UDP address: %s", err.Error())
	}
	//  establist a udp connection
	conn, err := net.DialUDP("udp", nil, udpAddress)
	if err != nil {
		return 0, fmt.Errorf("error creating udp connection: %s", err.Error())
	}
	defer conn.Close()

	// serialize
	serializedMessage, err := Serialize(message)
	if err != nil {
		return 0, fmt.Errorf("error serializing message: %s", err.Error())
	}
	numOfBytes := len(serializedMessage)
	// send the message
	_, err = conn.Write(serializedMessage)
	if err != nil {
		return 0, fmt.Errorf("error sending data: %s", err.Error())
	}
	return int64(numOfBytes), nil
}
