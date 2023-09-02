package main

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/buraksezer/consistent"
	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	datap "github.com/andrew-delph/my-key-store/datap"
)

type NodeState struct {
	Health bool
}

type MyDelegate struct {
	msgCh chan []byte
	state *NodeState
}

func GetMyDelegate() *MyDelegate {
	delegate := &MyDelegate{state: &NodeState{Health: validFSM}}
	delegate.msgCh = make(chan []byte)
	return delegate
}

func UpdateNodeHealth(health bool) error {
	logrus.Debug("UpdateNodeHealth >> ", health)
	delegate.state.Health = health
	err := clusterNodes.UpdateNode(0)
	if err != nil {
		logrus.Errorf("UpdateNode err = %v", err)
	}
	return err
}

func (d *MyDelegate) NotifyMsg(msg []byte) {
	d.msgCh <- msg
}

func (d *MyDelegate) NodeMeta(limit int) []byte {
	data, err := json.Marshal(d.state)
	if err != nil {
		logrus.Errorf("NodeMeta err = %v", err)
		return nil
	}
	logrus.Debug("NodeMeta >> ", d.state.Health)
	return data
}

func (d *MyDelegate) LocalState(join bool) []byte {
	// not use, noop
	return []byte("")
}

func (d *MyDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	// not use, noop
	return nil
}

func (d *MyDelegate) MergeRemoteState(buf []byte, join bool) {
	// not use
}

type MyEventDelegate struct {
	consistent *consistent.Consistent
	nodes      map[string]NodeClient
	mu         sync.RWMutex
}

type NodeClient struct {
	conn   *grpc.ClientConn
	client *datap.InternalNodeServiceClient
}

var partitionVerified = make(map[int]bool)

func GetMyEventDelegate() *MyEventDelegate {
	events := new(MyEventDelegate)
	events.consistent = GetHashRing()
	events.nodes = make(map[string]NodeClient)
	return events
}

func (events *MyEventDelegate) GetNodeClient(nodeName string) (datap.InternalNodeServiceClient, error) {
	events.mu.RLock()
	defer events.mu.RUnlock()
	nodeClient, exists := events.nodes[nodeName]
	if !exists {
		return nil, fmt.Errorf("Client does exist for node = %s", nodeName)
	}
	return *nodeClient.client, nil
}

func (events *MyEventDelegate) NotifyJoin(node *memberlist.Node) {
	var err error
	logrus.Infof("join %s", node.Name)
	events.mu.Lock()
	defer events.mu.Unlock()

	err = HandleNotifyUpdate(node)
	if err != nil {
		logrus.Errorf("NotifyLeave err = %v", err)
		return
	}

	conn, client, err := GetClient(node.Addr.String())
	if err != nil {
		logrus.Fatalf("GetClient err=%v", err)
	}

	events.nodes[node.Name] = NodeClient{conn: conn, client: client}
	err = AddVoter(node)
	if err != nil {
		logrus.Errorf("add voter err = %v", err)
	}

	// myPartions, err = GetMemberPartions(events.consistent, conf.Name)
	// if err != nil {
	// 	logrus.Error(err)
	// 	return
	// }

	// logrus.Debugf("myPartions %v", myPartions)
	// store.LoadPartitions(myPartions)
}

func (events *MyEventDelegate) NotifyLeave(node *memberlist.Node) {
	var err error
	logrus.Infof("leave %s", node.Name)
	events.mu.Lock()
	defer events.mu.Unlock()

	err = HandleNotifyUpdate(node)
	if err != nil {
		logrus.Errorf("NotifyLeave err = %v", err)
		return
	}

	RemoveNode(events.consistent, node)

	nodeClient, exists := events.nodes[node.Name]
	if exists {
		err := nodeClient.conn.Close()
		if err != nil {
			logrus.Errorf("NotifyLeave conn.Close() err = %v", err)
		}
	}

	delete(events.nodes, node.Name)

	err = RemoveServer(node)
	if err != nil {
		logrus.Errorf("remove server err = %v", err)
		return
	}

	// myPartions, err = GetMemberPartions(events.consistent, conf.Name)
	// if err != nil {
	// 	logrus.Error(err)
	// 	return
	// }
	// logrus.Debugf("myPartions %v", myPartions)
	// store.LoadPartitions(myPartions)
}

func (events *MyEventDelegate) NotifyUpdate(node *memberlist.Node) {
	err := HandleNotifyUpdate(node)
	if err != nil {
		logrus.Errorf("NotifyUpdate err = %v", err)
		return
	}
}

func HandleNotifyUpdate(node *memberlist.Node) error {
	var otherNode NodeState
	err := json.Unmarshal(node.Meta, &otherNode)
	if err != nil {
		logrus.Errorf("HandleNotifyUpdate error deserializing. err = %v", err)
		return err
	}

	if otherNode.Health {
		AddNode(events.consistent, node)
	} else {
		logrus.Warnf("HandleNotifyUpdate name = %s Health = %v", node.Name, otherNode.Health)
		RemoveNode(events.consistent, node)
	}

	return nil
}
