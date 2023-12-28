package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/cbergoon/merkletree"
	"github.com/gogo/status"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/andrew-delph/my-key-store/config"
	"github.com/andrew-delph/my-key-store/consensus"
	"github.com/andrew-delph/my-key-store/gossip"
	"github.com/andrew-delph/my-key-store/hashring"
	"github.com/andrew-delph/my-key-store/http"
	"github.com/andrew-delph/my-key-store/rpc"
	"github.com/andrew-delph/my-key-store/storage"
	"github.com/andrew-delph/my-key-store/utils"
)

type Manager struct {
	config                config.Config
	reqCh                 chan interface{}
	db                    storage.Storage
	httpServer            *http.HttpServer
	gossipCluster         *gossip.GossipCluster
	consensusCluster      *consensus.ConsensusCluster
	ring                  *hashring.Hashring
	rpcWrapper            *rpc.RpcWrapper
	myPartitions          *utils.IntSet
	consistencyController *ConsistencyController
	clientManager         *ClientManager

	debugTick         *time.Ticker
	epochTick         *time.Ticker
	CurrentEpoch      int64
	LastEpochUpdateId string
}

func NewManager(c config.Config) Manager {
	reqCh := make(chan interface{}, c.Manager.ReqChannelSize)

	httpServer := http.CreateHttpServer(c.Http, reqCh)
	gossipCluster := gossip.CreateGossipCluster(c.Gossip, reqCh)
	db := storage.NewBadgerStorage(c.Storage)
	// db := storage.NewLevelDbStorage(c.Storage)
	consensusCluster := consensus.CreateConsensusCluster(c.Consensus, reqCh)
	ring := hashring.CreateHashring(c.Manager, reqCh)

	rpcWrapper := rpc.CreateRpcWrapper(c.Rpc, reqCh)
	parts := utils.NewIntSet()

	clientManager := NewClientManager()

	consistencyController := NewConsistencyController(c.Manager.PartitionConcurrency, c.Manager.PartitionCount, reqCh)
	return Manager{
		config:                c,
		reqCh:                 reqCh,
		db:                    db,
		httpServer:            &httpServer,
		gossipCluster:         gossipCluster,
		consensusCluster:      consensusCluster,
		ring:                  ring,
		rpcWrapper:            rpcWrapper,
		myPartitions:          &parts,
		consistencyController: consistencyController,
		clientManager:         clientManager,
		debugTick:             time.NewTicker(time.Second * 5),
		epochTick:             time.NewTicker(time.Duration(c.Consensus.EpochTime) * time.Second),
	}
}

var currEpochLock sync.RWMutex

func (m *Manager) SetCurrentEpoch(Epoch int64) {
	currEpochLock.Lock()
	defer currEpochLock.Unlock()
	m.CurrentEpoch = Epoch
}

func (m *Manager) GetCurrentEpoch() int64 {
	currEpochLock.RLock()
	defer currEpochLock.RUnlock()
	return m.CurrentEpoch
}

func (m *Manager) StartManager() {
	if m.config.Manager.PartitionBuckets%2 != 0 {
		logrus.Fatalf("PartitionBuckets must be even. PartitionBuckets = %d", m.config.Manager.PartitionBuckets)
	}
	var err error
	go m.startWorkers()

	go m.rpcWrapper.StartRpcServer()

	err = m.consensusCluster.StartConsensusCluster()
	if err != nil {
		logrus.Fatal(err)
	}
	err = m.gossipCluster.Join()
	if err != nil {
		logrus.Fatal(err)
	}

	go m.httpServer.StartHttp()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	<-signals
	logrus.Warn("Received SIGTERM signal")

	// TODO add TrackTime for all requests...
	defer utils.TrackTime(time.Now(), 0, "GRACEFUL SHUTDOWN")

	err = m.consensusCluster.Snapshot()
	if err != nil {
		logrus.Errorf("Failed to Snapshot err = %v", err)
	}

	err = m.consensusCluster.Shutdown()
	if err != nil {
		logrus.Errorf("Failed to consensus Shutdown err = %v", err)
	}

	err = m.gossipCluster.Leave()
	if err != nil {
		logrus.Errorf("Failed to gossip Leave err = %v", err)
	}

	err = m.httpServer.Shutdown()
	if err != nil {
		logrus.Errorf("Failed to http Shutdown err = %v", err)
	}

	err = m.rpcWrapper.Stop()
	if err != nil {
		logrus.Errorf("Failed to rpc Stop err = %v", err)
	}

	err = m.stopWorkers()
	if err != nil {
		logrus.Errorf("Failed to stop manager workers err = %v", err)
	}

	err = m.db.Close()
	if err != nil {
		logrus.Errorf("Failed to db Close err = %v", err)
	} else {
		// logrus.Warn("DB CLOSE SUCCESS")
	}
}

func (m *Manager) startWorkers() {
	for i := 0; i < m.config.Manager.WokersCount; i++ {
		go m.startWorker(i)
	}
}

type StopWorkerTask struct {
	wg *sync.WaitGroup
}

func (m *Manager) stopWorkers() error {
	var wg sync.WaitGroup
	wg.Add(m.config.Manager.WokersCount)
	for i := 0; i < m.config.Manager.WokersCount; i++ {
		m.reqCh <- StopWorkerTask{wg: &wg}
	}
	wg.Wait() // TODO add a timeout
	close(m.reqCh)
	return nil
}

func (m *Manager) startWorker(workerId int) {
	logrus.Debugf("starting worker %d", workerId)

	for {
		select {
		case <-m.debugTick.C:
			// m.consensusCluster.Details()
			err := m.consensusCluster.IsHealthy()
			if err != nil {
				logrus.Debugf("IsHealthy err = %v", err)
			}

			err = m.consistencyController.IsHealthy()
			if err != nil {
				// logrus.Warnf("consistencyController health err= %v", err)
			}

			// logrus.Warnf("curr %v temp %v", len(m.ring.GetMembersNames(false)), len(m.ring.GetMembersNames(true)))
		case <-m.epochTick.C:

			if m.ring.HasTempMembers() == false {
				err := m.consensusCluster.UpdateFsm(m.GetCurrentEpoch()+1, m.ring.GetMembersNames(false), m.ring.GetMembersNames(true))
				if err != nil {
					logrus.Error("UpdateFsm err = %v", err)
				}
			}

		// case isLeader: <-m.consensusCluster.LeaderCh():
		case isLeader := <-m.consensusCluster.LeaderCh():
			// logrus.Warnf("worker LeaderChangeTask: %+v", task)

			logrus.Warn("members: ", m.gossipCluster.GetMembersNames())

			if !isLeader {
				continue
			}
			logrus.Warn("I AM THE LEADER")

			for _, member := range m.gossipCluster.GetMembers() {
				err := m.consensusCluster.AddVoter(member.Name, member.Addr.String())
				if err != nil {
					err = errors.Wrap(err, "gossip.JoinTask")
					logrus.Error(err)
					continue
				} else {
					logrus.Debugf("AddVoter success")
				}
			}
			if m.config.Manager.Operator == false {
				if m.GetCurrentEpoch() == int64(0) {
					err := m.consensusCluster.UpdateFsm(m.GetCurrentEpoch()+1, m.gossipCluster.GetMembersNames(), m.gossipCluster.GetMembersNames())
					if err != nil {
						logrus.Error("UpdateFsm err = %v", err)
					}
				}
				err := m.consensusCluster.UpdateFsm(m.GetCurrentEpoch(), m.gossipCluster.GetMembersNames(), m.gossipCluster.GetMembersNames())
				if err != nil {
					logrus.Warnf("PartitionsUpdateTask err = %v", err)
				}
			}

		case data, ok := <-m.reqCh:
			if !ok {
				logrus.Warn("Channel closed!")
				return
			}
			switch task := data.(type) {

			case StopWorkerTask:
				logrus.Warn("StopWorkerTask!")
				defer task.wg.Done()
				return
			default:
				if m.config.Manager.ThreadRequests {
					go m.handleTask(data)
				} else {
					m.handleTask(data)
				}
			}

		}
	}
}

func (m *Manager) handleTask(data interface{}) {
	switch task := data.(type) {

	case StopWorkerTask:
		defer task.wg.Done()
		return

	case rpc.PartitionsHealthCheckTask:
		// logrus.Warn("PartitionsHealthCheckTask")
		err := m.consistencyController.IsHealthy()
		task.ResCh <- err

	case rpc.UpdateMembersTask:
		var err error

		if m.consensusCluster.Isleader() == false {
			task.ResCh <- true
			return
		}
		if m.ring.CompareMembers(task.Members, task.TempMembers) == true {
			// logrus.Warn("members already changed")
			task.ResCh <- true
			return
		}

		err = m.consensusCluster.UpdateFsm(m.GetCurrentEpoch(), task.Members, task.TempMembers)
		if err != nil {
			logrus.Error("UpdateFsm err = %v", err)
			task.ResCh <- err
			return
		}

		task.ResCh <- true

	case rpc.UpdateEpochTask:
		var err error

		if m.consensusCluster.Isleader() == false {
			task.ResCh <- true
			return
		}

		if m.LastEpochUpdateId == task.UpdateId {
			task.ResCh <- true
			return
		}

		logrus.Warnf("UpdateEpochTask %s", task.UpdateId)

		err = m.consensusCluster.UpdateFsm(m.GetCurrentEpoch()+1, m.ring.GetMembersNames(false), m.ring.GetMembersNames(true))
		if err != nil {
			logrus.Warnf("UpdateEpochTask UpdateMembers err = %v", err)
			task.ResCh <- err
		}

		err = m.consensusCluster.UpdateFsm(m.GetCurrentEpoch()+1, m.ring.GetMembersNames(false), m.ring.GetMembersNames(true))
		if err != nil {
			logrus.Warnf("UpdateEpochTask UpdateMembers err = %v", err)
			task.ResCh <- err
		}

		m.LastEpochUpdateId = task.UpdateId

		task.ResCh <- true

	case http.HealthTask:
		err := m.consensusCluster.IsHealthy()
		if err != nil {
			logrus.Warnf("HealthTask err = %v", err)
			task.ResCh <- err
			return
		}

		task.ResCh <- true
	case http.ReadyTask:
		err := m.consensusCluster.IsHealthy()
		if err != nil {
			logrus.Debugf("IsHealthy err = %v", err)
			task.ResCh <- err
			return
		}

		// err = m.ring.IsHealthy()
		// if err != nil {
		// 	logrus.Warnf("ring.IsHealthy err = %v", err)
		// 	task.ResCh <- err
		// 	continue
		// }

		// err = m.consistencyController.IsBusy()
		// if err != nil {
		// 	logrus.Warnf("HealthTask err = %v", err)
		// 	task.ResCh <- err
		// 	continue
		// }

		task.ResCh <- true

	case http.SetTask:
		logrus.Debugf("worker SetTask: %+v", task)
		members, err := m.SetRequest(task.Key, task.Value)
		errorStr := ""
		if err != nil {
			errorStr = err.Error()
		}
		task.ResCh <- http.SetResponse{Error: errorStr, Members: members}

	case http.GetTask:
		logrus.Debugf("worker GetTask: %+v", task)
		value, failed_members, err := m.GetRequest(task.Key)
		var valueStr string
		if value != nil {
			valueStr = value.Value
		}
		errorStr := ""
		if err != nil {
			errorStr = err.Error()
		}
		task.ResCh <- http.GetResponse{Value: valueStr, Error: errorStr, Failed_members: failed_members}

	case gossip.JoinTask:
		// logrus.Warnf("worker JoinTask: %+v", task)

		err := m.consensusCluster.AddVoter(task.Name, task.IP)
		if err != nil {
			err = errors.Wrap(err, "gossip.JoinTask")
			logrus.Error(err)
		} else {
			// logrus.Infof("AddVoter success")
		}

		_, rpcClient, err := m.rpcWrapper.CreateRpcClient(task.IP)
		if err != nil {
			err = errors.Wrap(err, "gossip.JoinTask")
			logrus.Fatal(err)
			return
		}
		m.clientManager.AddClient(task.Name, rpcClient)

		if m.config.Manager.Operator == false {
			err = m.consensusCluster.UpdateFsm(m.GetCurrentEpoch(), m.gossipCluster.GetMembersNames(), m.gossipCluster.GetMembersNames())
			if err != nil {
				logrus.Warnf("JoinTask UpdateMembers err = %v", err)
			}
		}

	case gossip.LeaveTask:
		// logrus.Warnf("worker LeaveTask: %+v", task)
		m.clientManager.RemoveClient(task.Name)
		m.consensusCluster.RemoveServer(task.Name)
		if m.config.Manager.Operator == false {
			err := m.consensusCluster.UpdateFsm(m.GetCurrentEpoch(), m.gossipCluster.GetMembersNames(), m.gossipCluster.GetMembersNames())
			if err != nil {
				logrus.Warnf("JoinTask UpdateMembers err = %v", err)
			}
		}

	case consensus.FsmTask:
		logrus.Warnf("FsmTask Epoch %v Members %v TempMembers %v", task.Epoch, len(task.Members), len(task.TempMembers))
		m.SetCurrentEpoch(task.Epoch)
		m.consistencyController.PublishEpoch(task.Epoch)

		m.ring.SetRingMembers(task.Members, task.TempMembers)
		task.ResCh <- true

	case rpc.SetValueTask:
		logrus.Debugf("worker SetValueTask: %+v", task)

		if task.Value.Epoch < m.GetCurrentEpoch()-1 {
			task.ResCh <- errors.New("cannot set lagging epoch")
			return
		}
		err := m.SetValue(task.Value)
		if err != nil {
			logrus.Warnf("SetValue err = %v", err)
			task.ResCh <- err
		} else {
			task.ResCh <- true
		}

	case rpc.GetValueTask:
		logrus.Debugf("worker GetValueTask: %+v", task)
		// value, err := m.db.Get([]byte(task.Key))
		value, err := m.GetValue(task.Key)
		if err == storage.KEY_NOT_FOUND { // TODO if the nodes partition is not up to date it should not count as response
			task.ResCh <- nil
		} else if err != nil {
			task.ResCh <- err
		} else {
			task.ResCh <- value
		}

	case rpc.StreamBucketsTask: // TODO test this is returning right values
		logrus.Debugf("worker StreamBucketsTask: %+v", task)
		var buckets []int32 = task.Buckets
		if len(buckets) == 0 {
			for i := 0; i < m.config.Manager.PartitionBuckets; i++ {
				buckets = append(buckets, int32(i))
			}
		}
		for _, bucket := range buckets {
			index1, err := BuildEpochIndex(int(task.PartitionId), uint64(bucket), task.LowerEpoch, "")
			if err != nil {
				logrus.Fatal(err)
				continue
			}
			index2, err := BuildEpochIndex(int(task.PartitionId), uint64(bucket), task.UpperEpoch, "")
			if err != nil {
				logrus.Fatal(err)
				continue
			}
			it := m.db.NewIterator(
				[]byte(index1),
				[]byte(index2),
				false,
			)
			for !it.IsDone() {
				_, _, epoch, key, err := ParseEpochIndex(string(it.Key()))
				if err != nil {
					logrus.Fatal(err)
					continue
				}
				timestamp, err := utils.DecodeBytesToInt64(it.Value())
				if err != nil {
					logrus.Fatal(err)
					continue
				}

				task.ResCh <- &rpc.RpcValue{Key: key, Epoch: epoch, UnixTimestamp: timestamp}
				it.Next()
			}
			it.Release()
		}

		close(task.ResCh)

	case VerifyPartitionEpochRequestTask:
		logrus.Debugf("worker VerifyPartitionEpochRequestTask: %+v", task.PartitionId)
		err := m.VerifyEpoch(task.PartitionId, task.Epoch)

		if err != nil {
			task.ResCh <- err
		} else {
			task.ResCh <- VerifyPartitionEpochResponse{Valid: true}
		}

	case rpc.GetEpochTreeObjectTask:
		logrus.Debugf("worker GetPartitionEpochObjectTask: %+v", task)
		index, err := BuildEpochTreeObjectIndex(int(task.PartitionId), task.LowerEpoch)
		if err != nil {
			task.ResCh <- err
			return
		}

		epochTreeObjectBytes, err := m.db.Get([]byte(index))
		if err != nil {
			// logrus.Warnf("GetEpochTreeObjectTask err = %v index %v  active: %v", err, index, m.consistencyController.IsPartitionActive(int(task.PartitionId)))
			err = errors.Wrapf(err, "active: %v", m.consistencyController.IsPartitionActive(int(task.PartitionId)))
			task.ResCh <- err
			return
		}

		epochTreeObject := &rpc.RpcEpochTreeObject{}
		err = proto.Unmarshal(epochTreeObjectBytes, epochTreeObject)
		if err != nil {
			task.ResCh <- err
			return
		}
		task.ResCh <- epochTreeObject

	case rpc.GetEpochTreeLastValidObjectTask:
		logrus.Debugf("worker GetEpochTreeLastValidObjectTask: %+v", task)
		epochTreeObjectLastValid, err := m.GetEpochTreeLastValid(task.PartitionId)
		if err != nil {
			task.ResCh <- err
		} else if epochTreeObjectLastValid == nil {
			task.ResCh <- errors.New("no valid EpochTreeObject for partition")
		} else {
			task.ResCh <- epochTreeObjectLastValid
		}

	case SyncPartitionTask:
		logrus.Debugf("worker SyncPartitionTask: %+v", task.PartitionId)
		epochTreeObjectLastValid, err := m.GetEpochTreeLastValid(task.PartitionId)
		if err != nil {
			task.ResCh <- errors.Wrap(err, "GetEpochTreeLastValid")
			return
		} else if epochTreeObjectLastValid != nil && epochTreeObjectLastValid.LowerEpoch >= task.UpperEpoch { // TODO validate this is the correct compare
			task.ResCh <- nil
			return
		}

		lastValidEpoch := int64(0)
		if epochTreeObjectLastValid != nil {
			lastValidEpoch = epochTreeObjectLastValid.LowerEpoch
		}

		logrus.Debugf("sync lastValidEpoch %d", lastValidEpoch)

		// find most healthy node
		err = m.PoliteStreamRequest(int(task.PartitionId), lastValidEpoch, task.UpperEpoch+1, nil)

		if err != nil {
			logrus.Debug(err)
			task.ResCh <- SyncPartitionResponse{Valid: false, LowerEpoch: lastValidEpoch, UpperEpoch: task.UpperEpoch + 1}
		} else {
			task.ResCh <- SyncPartitionResponse{Valid: true, LowerEpoch: lastValidEpoch, UpperEpoch: task.UpperEpoch + 1}
		}

	case hashring.RingUpdateTask:
		// logrus.Warnf("worker MembersUpdateTask #%+v", len(task.Partitions))

		currPartitions := utils.NewIntSet().From(task.Partitions)
		m.consistencyController.HandleHashringChange(currPartitions)

		task.ResCh <- true

	default:
		logrus.Panicf("worker unkown task type: %v", reflect.TypeOf(task))

	}
}

func (m *Manager) SetRequest(key, value string) ([]string, error) {
	nodes, err := m.ring.GetClosestN(key, m.config.Manager.ReplicaCount, true)
	if err != nil {
		return nil, err
	}

	unixTimestamp := time.Now().Unix()
	setReq := &rpc.RpcValue{Key: key, Value: value, Epoch: m.GetCurrentEpoch(), UnixTimestamp: unixTimestamp}

	responseCh := make(chan *rpc.RpcStandardObject, m.config.Manager.ReplicaCount)
	errorCh := make(chan error, m.config.Manager.ReplicaCount)

	var members []string
	var statuses []codes.Code

	clientErrors := 0

	for _, member := range nodes {

		members = append(members, member.String())

		client, err := m.clientManager.GetClient(member.String())
		if err != nil {
			clientErrors++
			errorCh <- err
			logrus.Debugf("SetRequest err = %v", err)
			continue
		}

		go func() {
			// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			// defer cancel()
			ctx := context.Background()
			res, err := client.SetRequest(ctx, setReq)
			if err != nil {
				errorCh <- err
			} else if res != nil {
				responseCh <- res
			}
		}()
	}

	timeout := time.After(time.Second * time.Duration(m.config.Manager.DefaultTimeout))
	responseCount := 0
	errorCount := 0

	for responseCount < m.config.Manager.WriteQuorum {
		select {
		case <-responseCh:
			responseCount++
		case err := <-errorCh:
			st, ok := status.FromError(err)
			if ok {
				statuses = append(statuses, st.Code())
			}
			errorCount++
			// logrus.Errorf("SetRequest errorCh: %v", err)
			_ = err // Handle error if necessary
		case <-timeout:
			return members, fmt.Errorf("SET: Timeout. responseCount = %d errorCount = %d clientErrors = %d statuses = %v", responseCount, errorCount, clientErrors, statuses)
		}
	}
	if responseCount < m.config.Manager.WriteQuorum {
		return members, fmt.Errorf("failed WriteQuorum. responseCount = %d", responseCount)
	} else {
		return members, nil
	}
}

func (m *Manager) GetRequest(key string) (*rpc.RpcValue, []string, error) {
	nodes, err := m.ring.GetClosestN(key, m.config.Manager.ReplicaCount, true)
	if err != nil {
		return nil, nil, err
	}

	getReq := &rpc.RpcGetRequestMessage{Key: key}
	responseCh := make(chan *rpc.RpcValue, m.config.Manager.ReplicaCount)
	errorCh := make(chan error, m.config.Manager.ReplicaCount)
	var statuses []codes.Code
	var failed_members []string
	clientErrors := 0
	for _, member := range nodes {

		client, err := m.clientManager.GetClient(member.String())
		if err != nil {
			errorCh <- err
			clientErrors++
			logrus.Debugf("GetRequest err = %v", err)
			continue
		}

		failed_members = append(failed_members, member.String())

		go func() {
			// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			// ctx, cancel := context.WithCancel(context.Background())
			// defer cancel()
			ctx := context.Background()
			res, err := client.GetRequest(ctx, getReq)
			if err != nil {
				st, ok := status.FromError(err)
				statuses = append(statuses, st.Code())
				if ok && st.Code() != codes.NotFound {
					// logrus.Errorf("GetRequest name: %v code %v", member.Name, st.Code())
				}
				errorCh <- err
			} else {
				responseCh <- res
			}
		}()
	}

	responseCount := 0
	var recentValue *rpc.RpcValue
	timeout := time.After(time.Second * time.Duration(m.config.Manager.DefaultTimeout))
	for responseCount < m.config.Manager.ReadQuorum {
		select {
		case res := <-responseCh:

			if res == nil {
				logrus.Panic("GET res is nil!")
			}

			responseCount++ // Include not found as a valid response?

			if recentValue == nil {
				recentValue = res
			} else if recentValue.Epoch <= res.Epoch && recentValue.UnixTimestamp < res.UnixTimestamp {
				recentValue = res
			}
		case err := <-errorCh:
			// logrus.Errorf("GetRequest errorCh %v", err)
			// st, ok := status.FromError(err)
			// if ok && st.Code() != codes.NotFound {
			// 	logrus.Errorf("GetRequest st %+v code %v", *st, st.Code())
			// }
			_ = err

		case <-timeout:
			return nil, failed_members, fmt.Errorf("GET TIMEOUT: responseCount = %d clientErrors = %d nodes = %d statuses = %v failed_members = %v", responseCount, clientErrors, len(nodes), statuses, failed_members)
		}
	}
	if responseCount < m.config.Manager.ReadQuorum {
		return nil, failed_members, fmt.Errorf("failed ReadQuorum. responseCount = %d", responseCount)
	} else if recentValue == nil {
		return nil, failed_members, nil
	} else {
		return recentValue, failed_members, nil
	}
}

func (m *Manager) EpochTreeObjectRequest(partitionId int, epoch int64, timeout time.Duration) ([]*rpc.RpcEpochTreeObject, error) {
	nodes, err := m.ring.GetClosestNForPartition(partitionId, m.config.Manager.ReplicaCount, true)
	if err != nil {
		return nil, err
	}

	treeReq := &rpc.RpcEpochTreeObject{Partition: int32(partitionId), LowerEpoch: epoch, UpperEpoch: epoch + 1}
	responseCh := make(chan *rpc.RpcEpochTreeObject, m.config.Manager.ReplicaCount)
	errorCh := make(chan error, m.config.Manager.ReplicaCount)

	for _, member := range nodes {

		client, err := m.clientManager.GetClient(member.String())
		if err != nil {
			errorCh <- err
			logrus.Debugf("EpochTreeObjectRequest err = %v", err)
			continue
		}

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			res, err := client.GetEpochTree(ctx, treeReq)
			if err != nil {
				// logrus.Errorf("GetEpochTree %s err %v partitionId %d epoch %d", member.Name, err, partitionId, epoch)
				errorCh <- err
			} else {
				responseCh <- res
			}
		}()
	}

	var otherTrees []*rpc.RpcEpochTreeObject
	for i := 0; i < len(nodes); i++ {
		select {
		case res := <-responseCh:
			otherTrees = append(otherTrees, res)
		case err := <-errorCh:
			logrus.Debugf("GetEpochTree err = %v", err)
		}
	}
	return otherTrees, nil
}

func (m *Manager) getKeyBucket(key string) uint64 {
	keyBytes := []byte(key)
	hash := sha256.Sum256(keyBytes)
	bucket := binary.BigEndian.Uint64(hash[:8]) % uint64(m.config.Manager.PartitionBuckets)
	return bucket
}

func (m *Manager) SetValue(value *rpc.RpcValue) error {
	keyBytes := []byte(value.Key)
	timestampBytes, err := utils.EncodeInt64ToBytes(value.UnixTimestamp)
	if err != nil {
		return err
	}
	valueData, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	partitionId := m.ring.FindPartitionID(keyBytes)
	bucket := m.getKeyBucket(value.Key)
	epochIndex, err := BuildEpochIndex(partitionId, bucket, value.Epoch, value.Key)
	if err != nil {
		return err
	}
	keyIndex, err := BuildKeyIndex(value.Key)
	if err != nil {
		return err
	}
	trx := m.db.NewTransaction(true)
	defer trx.Discard()
	existingBytes, err := trx.Get([]byte(keyIndex))
	if err == nil {
		existingValue := &rpc.RpcValue{}
		err = proto.Unmarshal(existingBytes, existingValue)
		if err != nil {
			return err
		}
		// logrus.Warnf("existing value found. %v %v / %v %v / %v %v", existingValue.Epoch, value.Epoch, existingValue.UnixTimestamp, value.UnixTimestamp, existingValue.Epoch >= value.Epoch, existingValue.UnixTimestamp > value.UnixTimestamp)
		if existingValue.Epoch >= value.Epoch && existingValue.UnixTimestamp > value.UnixTimestamp {
			return errors.New("a newer value already exists")
		}
	}

	trx.Set([]byte(keyIndex), valueData)
	trx.Set([]byte(epochIndex), timestampBytes)
	return trx.Commit()
}

func (m *Manager) GetValue(key string) (*rpc.RpcValue, error) {
	keyIndex, err := BuildKeyIndex(key)
	if err != nil {
		return nil, err
	}

	valueBytes, err := m.db.Get([]byte(keyIndex))
	if err != nil {
		return nil, err
	}
	value := &rpc.RpcValue{}
	err = proto.Unmarshal(valueBytes, value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (m *Manager) GetEpochTreeLastValid(partitionId int32) (*rpc.RpcEpochTreeObject, error) {
	index1, err := BuildEpochTreeObjectIndex(int(partitionId), 0)
	if err != nil {
		return nil, errors.Wrap(err, "BuildEpochTreeObjectIndex")
	}
	index2, err := BuildEpochTreeObjectIndex(int(partitionId), m.GetCurrentEpoch())
	if err != nil {
		return nil, errors.Wrap(err, "BuildEpochTreeObjectIndex")
	}
	it := m.db.NewIterator(
		[]byte(index1),
		[]byte(index2),
		true,
	)
	defer it.Release()
	for !it.IsDone() {
		epochTreeObjectBytes := it.Value()
		epochTreeObject := &rpc.RpcEpochTreeObject{}
		err = proto.Unmarshal(epochTreeObjectBytes, epochTreeObject)
		if err != nil {
			return nil, errors.WithMessagef(err, "RpcEpochTreeObject Unmarshal. epochTreeObjectBytes = %v", epochTreeObjectBytes)
		}

		if epochTreeObject.Valid {
			return epochTreeObject, nil
		}
		it.Next()
	}
	return &rpc.RpcEpochTreeObject{LowerEpoch: -1, UpperEpoch: -1, Valid: false, Partition: partitionId}, nil
}

type MemberEpochTreeLastValid struct {
	member             string
	epochTreeLastValid *rpc.RpcEpochTreeObject
}

func (m *Manager) EpochTreeLastValidRequest(partitionId int32, timeout time.Duration) ([]MemberEpochTreeLastValid, error) {
	nodes, err := m.ring.GetClosestNForPartition(int(partitionId), m.config.Manager.ReplicaCount, false)
	if err != nil {
		return nil, err
	}

	treeReq := &rpc.RpcEpochTreeObject{Partition: int32(partitionId)}
	responseCh := make(chan MemberEpochTreeLastValid, m.config.Manager.ReplicaCount)
	errorCh := make(chan error, m.config.Manager.ReplicaCount)

	for _, member := range nodes {

		client, err := m.clientManager.GetClient(member.String())
		if err != nil {
			errorCh <- err
			logrus.Debugf("EpochTreeLastValidRequest err = %v", err)
			continue
		}

		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			res, err := client.GetEpochTreeLastValid(ctx, treeReq)
			if err != nil {
				errorCh <- err
			} else {
				responseCh <- MemberEpochTreeLastValid{member: member.String(), epochTreeLastValid: res}
			}
		}()
	}

	var membersLastValid []MemberEpochTreeLastValid
	for i := 0; i < len(nodes); i++ {
		select {
		case res := <-responseCh:
			membersLastValid = append(membersLastValid, res)
		case err := <-errorCh:
			logrus.Debugf("EpochTreeLastValidRequest err = %v", err)
		}
	}
	return membersLastValid, nil
}

func (m *Manager) SyncPartitionRequest(member string, partitionId int32, lowerEpoch int64, upperEpoch int64, buckets []int32, timeout time.Duration) error {
	logrus.Debugf("CLIENT SyncPartitionRequest")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req := &rpc.RpcStreamBucketsRequest{Partition: partitionId, LowerEpoch: lowerEpoch, UpperEpoch: upperEpoch, Buckets: buckets}
	client, err := m.clientManager.GetClient(member)
	if err != nil {
		return err
	}
	streamClient, err := client.StreamBuckets(ctx, req)
	if err != nil {
		return errors.Wrap(err, "StreamBuckets request")
	}
	for {
		value, err := streamClient.Recv()
		if err == io.EOF {
			logrus.Debugf("CLIENT SyncPartitionRequest completed.")
			return nil
		} else if err != nil {
			return errors.Wrap(err, "StreamBuckets Recv")
		}

		// TODO change to get from epoch index.
		// if index value is less. write it.
		// then check the value. if the current value timestamp is greater then request from nodes
		bucket := m.getKeyBucket(value.Key)
		epochIndex, err := BuildEpochIndex(int(partitionId), bucket, value.Epoch, value.Key)
		if err != nil {
			return err
		}

		epochBytes, err := m.db.Get([]byte(epochIndex))
		if epochBytes != nil {
			timestamp, err := utils.DecodeBytesToInt64(epochBytes)
			if err == nil && timestamp >= value.UnixTimestamp {
				logrus.Debugf("epochIndex ALREADY SYNCED~~~~~~~~~~~~~~~ KEY = %s", value.Key)
				continue
			}
		}

		myValue, err := m.GetValue(value.Key)
		if myValue != nil && myValue.UnixTimestamp >= value.UnixTimestamp {
			logrus.Debugf("GetValue ALREADY SYNCED!!!!!!!!!!!!!!!! KEY = %s", value.Key)
		} else {
			getReq := &rpc.RpcGetRequestMessage{Key: value.Key}

			syncedValue, err := client.GetRequest(context.Background(), getReq)
			if err != nil {
				logrus.Errorf("FAILED TO SYNC KEY = %s err = %v", value.Key, err)
				continue
			}
			if syncedValue == nil {
				logrus.Errorf("SYNC NOT FOUND KEY = %s err = %v", value.Key, err)
				continue
			}
			err = m.SetValue(syncedValue)
			if err != nil {
				logrus.Errorf("FAILED WRITE SYNC KEY = %s err = %v", syncedValue.Key, err)
				continue
			} else {
				logrus.Debugf("SYNC SUCCESS KEY = %s", syncedValue.Key)
			}
		}

		// write the epochIndex value...
		timestampBytes, err := utils.EncodeInt64ToBytes(value.UnixTimestamp)
		if err != nil {
			logrus.Fatal("FAILED TO ENCOUDE UnixTimestamp IN SYNC")
		}
		err = m.db.Put([]byte(epochIndex), timestampBytes)
		if err != nil {
			logrus.Fatal("FAILED TO PUT EpochIndex IN SYNC")
		}

		// verify each epoch from bottom up

		// If I have key and greater timestamp. ignore.
		// else request the key and write to my db.
	}
}

func (m *Manager) VerifyEpoch(PartitionId int, Epoch int64) error {
	var err error
	var myTree *merkletree.MerkleTree
	var otherTree *merkletree.MerkleTree
	var partitionEpochObject *rpc.RpcEpochTreeObject
	var index string
	var diff []int32
	var data []byte

	if Epoch < 0 {
		return nil
	}
	if m.consistencyController.IsPartitionActive(PartitionId) == false {
		// logrus.Warn("VERIFY PARTITION THAT IS NOT ACTIVE!")
		// return nil
	}

	index, err = BuildEpochTreeObjectIndex(PartitionId, Epoch)
	if err != nil {
		return err
	}

	epochTreeObjectBytes, err := m.db.Get([]byte(index))
	if err == nil {
		epochTreeObject := &rpc.RpcEpochTreeObject{}
		err = proto.Unmarshal(epochTreeObjectBytes, epochTreeObject)
		if err == nil && epochTreeObject.Valid {
			// logrus.Warn("Fetch EpochTreeObject is valid")
			return nil
		}
	}

	partitionLabel := fmt.Sprintf("%d", PartitionId)
	logrus.Debug("partitionLabel = ", partitionLabel)
	epochLabel := fmt.Sprintf("%d", Epoch)
	logrus.Debug("epochLabel = ", epochLabel)
	partitionVerifyEpochAttemptsGague.WithLabelValues(partitionLabel, epochLabel).Inc()

	myTree, err = m.RawPartitionMerkleTree(PartitionId, Epoch, Epoch+1)
	if err != nil {
		return err
	}
	// serialize
	partitionEpochObject, err = MerkleTreeToPartitionEpochObject(myTree, PartitionId, Epoch, Epoch+1)
	if err != nil {
		return err
	}
	data, err = proto.Marshal(partitionEpochObject)
	if err != nil {
		return err
	}

	// save to db

	err = m.db.Put([]byte(index), data)
	if err != nil {
		return err
	}
	partitionEpochObjectBuilt.WithLabelValues(partitionLabel, epochLabel).Set(1)

	var epochTreeObjects []*rpc.RpcEpochTreeObject

	epochTreeObjects, err = m.EpochTreeObjectRequest(PartitionId, Epoch, time.Second*20)
	if err != nil {
		return err
	}

	// if len(epochTreeObjects) < m.config.Manager.ReadQuorum {
	// 	err = errors.Errorf("need more trees #%d", len(epochTreeObjects))
	// 	continue
	// }
	// TODO do we need this?

	// compare the difference to the otherTree
	validCount := 0
	diffSet := utils.NewInt32Set()
	validDiff := false
	for _, epochTreeObject := range epochTreeObjects {
		otherTree, err = EpochTreeObjectToMerkleTree(epochTreeObject)
		if err != nil {
			logrus.Error(err)
			continue
		}
		diff, err = DifferentMerkleTreeBuckets(myTree, otherTree)
		if err != nil {
			logrus.Error(err)
			continue
		}
		diffSet.From(diff)

		if len(diff) == 0 {
			validCount++
		} else {
			// logrus.Warnf("diff %d items count: my %d other %d other_valid %v", len(diff), partitionEpochObject.Items, epochTreeObject.Items, epochTreeObject.Valid)
			if epochTreeObject.Valid == true {
				// logrus.Error(errors.New("failed valid check against valid tree"))
				validDiff = true
			}
		}
	}

	if validCount >= m.config.Manager.ReplicaCount || validDiff == true {
		partitionEpochObject.Valid = true
		data, err = proto.Marshal(partitionEpochObject)
		if err != nil {
			return err
		}
		err = m.db.Put([]byte(index), data)
		if err != nil {
			return err
		}
		// logrus.Debugf("verified P=%v E=%v", partitionEpochObject.Partition, partitionEpochObject.LowerEpoch)
		partitionEpochObjectBuilt.WithLabelValues(partitionLabel, epochLabel).Set(1)
		partitionEpochObjectVerified.WithLabelValues(partitionLabel, epochLabel).Set(1)
		return nil
	} else {
		err = m.PoliteStreamRequest(int(partitionEpochObject.Partition), Epoch, Epoch+1, diffSet.List())
		err = errors.Errorf("validCount= %d<%d Epoch %d trees %d diffSet= %v err = %v", validCount, m.config.Manager.ReadQuorum, Epoch, len(epochTreeObjects), len(diffSet.List()), err)
		return err
	}
}

func (m *Manager) PoliteStreamRequest(PartitionId int, LowerEpoch, UpperEpoch int64, buckets []int32) error {
	// find most healthy node
	// then stream from it
	membersLastValid, err := m.EpochTreeLastValidRequest(int32(PartitionId), time.Second*10)
	if err != nil {
		return err
	}

	sort.Slice(membersLastValid, func(i, j int) bool { // sort most healthy first
		return membersLastValid[i].epochTreeLastValid.LowerEpoch > membersLastValid[j].epochTreeLastValid.LowerEpoch
	})

	if len(membersLastValid) == 0 {
		return errors.New("membersLastValid is 0")
	}

	for _, lastValid := range membersLastValid {
		// logrus.Warnf("sync name %s lastValid %d", lastValid.member.Name, lastValid.epochTreeLastValid.LowerEpoch)
		err := m.SyncPartitionRequest(lastValid.member, int32(PartitionId), LowerEpoch, UpperEpoch, buckets, time.Second*60*3)
		if err != nil {
			// logrus.Errorf("SyncPartitionRequest err = %v", err)
		} else if lastValid.epochTreeLastValid.LowerEpoch >= UpperEpoch {
			// return nil
		}
	}

	return fmt.Errorf("PoliteStreamRequest no valid member. membersLastValid %d", len(membersLastValid))
}
