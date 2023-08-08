package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"
	"github.com/serialx/hashring"
	"github.com/sirupsen/logrus"
)

type MyEventDelegate struct {
	consistent *hashring.HashRing
	nodes      map[string]*memberlist.Node
}

func GetMyEventDelegate() *MyEventDelegate {
	events := new(MyEventDelegate)

	events.consistent = hashring.New([]string{})

	events.nodes = make(map[string]*memberlist.Node)

	return events
}

func (events *MyEventDelegate) NotifyJoin(node *memberlist.Node) {

	logrus.Infof("join %s", node.Name)

	events.consistent = events.consistent.AddNode(node.Name)

	events.nodes[node.Name] = node
}

func (events *MyEventDelegate) NotifyLeave(node *memberlist.Node) {

	logrus.Infof("leave %s", node.Name)

	events.consistent = events.consistent.RemoveNode(node.Name)

	delete(events.nodes, node.Name)
}
func (events *MyEventDelegate) NotifyUpdate(node *memberlist.Node) {
	// skip
}

func (events *MyEventDelegate) SendSetMessage(key, value string) error {

	ackId := uuid.New().String()

	setMsg := NewSetMessage(key, value, ackId)

	nodes, ok := events.consistent.GetNodes(key, totalReplicas)

	ackChannel := make(chan *MessageHolder, totalReplicas)
	defer close(ackChannel)

	setAckChannel(ackId, ackChannel)
	defer deleteAckChannel(ackId)

	if !ok {
		return fmt.Errorf("no node available size=%d", events.consistent.Size())
	}

	for _, nodeName := range nodes {

		logrus.Debugf("node1 search %s => %s", key, nodeName)

		node := events.nodes[nodeName]

		bytes, err := EncodeHolder(setMsg)

		if err != nil {
			return fmt.Errorf("FAILED TO ENCODE: %v", err)
		}

		err = clusterNodes.SendReliable(node, bytes)

		if err != nil {
			return fmt.Errorf("FAILED TO SEND: %v", err)
		}
	}

	ackSet := make(map[string]bool)

	timeout := time.After(defaultTimeout)

	for {
		select {
		case ackMessageHolder := <-ackChannel:
			ackSet[ackMessageHolder.SenderName] = true
			if len(ackSet) >= writeResponse {
				return nil
			}
		case <-timeout:
			logrus.Warn("TIME OUT REACHED!!!", "len(ackSet)", len(ackSet))
			return fmt.Errorf("timeout waiting for acknowledgements")
		}
	}

}

func (events *MyEventDelegate) SendGetMessage(key string) (string, error) {

	ackId := uuid.New().String()

	getMsg := NewGetMessage(key, ackId)

	nodes, ok := events.consistent.GetNodes(key, totalReplicas)

	ackChannel := make(chan *MessageHolder, totalReplicas)
	defer close(ackChannel)

	setAckChannel(ackId, ackChannel)
	defer deleteAckChannel(ackId)

	if !ok {
		return "", fmt.Errorf("no node available size=%d", events.consistent.Size())
	}

	for _, nodeName := range nodes {

		node := events.nodes[nodeName]

		bytes, err := EncodeHolder(getMsg)

		if err != nil {
			return "", fmt.Errorf("FAILED TO ENCODE: %v", err)
		}

		err = clusterNodes.SendReliable(node, bytes)

		if err != nil {
			return "", fmt.Errorf("FAILED TO SEND: %v", err)
		}
	}

	ackSet := make(map[string]int)

	timeout := time.After(defaultTimeout)

	for {
		select {
		case ackMessageHolder := <-ackChannel:

			ackMessage := &AckMessage{}
			err := ackMessage.Decode(ackMessageHolder.MessageBytes)
			if err != nil {
				return "", fmt.Errorf("failed to Decode AckMessage: %v", err)
			}

			ackValue := ackMessage.Value
			ackSet[ackValue]++

			if ackSet[ackValue] == readResponse {
				return ackValue, nil
			}
		case <-timeout:
			logrus.Warn("TIME OUT REACHED!!!")
			return "", fmt.Errorf("timeout waiting for acknowledgements")
		}
	}

}

func (events *MyEventDelegate) SendAckMessage(value, ackId, senderName string, success bool) error {

	ackMsg := NewAckMessage(ackId, success, value)

	node := events.nodes[senderName]

	bytes, err := EncodeHolder(ackMsg)

	if err != nil {
		return fmt.Errorf("FAILED TO ENCODE: %v", err)
	}

	err = clusterNodes.SendReliable(node, bytes)

	if err != nil {
		return fmt.Errorf("FAILED TO SEND: %v", err)
	}

	return nil
}
