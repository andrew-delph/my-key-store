package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/rotisserie/eris"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestConfigDefault(t *testing.T) {
	config := GetDefaultConfig()

	// manager config
	assert.NotEqualValues(t, 0, config.Manager.ReplicaCount, "ReplicaCount wrong value")
	assert.NotEqualValues(t, 0, config.Manager.WriteQuorum, "WriteQuorum wrong value")
	assert.NotEqualValues(t, 0, config.Manager.ReadQuorum, "ReadQuorum wrong value")
	assert.NotEqualValues(t, 0, config.Manager.PartitionCount, "PartitionCount wrong value")
	assert.NotEqualValues(t, 0, config.Manager.PartitionBuckets, "PartitionBuckets wrong value")
	assert.EqualValues(t, "/data/storage", config.Manager.DataPath, "DataPath wrong value")
	assert.NotEqualValues(t, 0, config.Manager.DefaultTimeout, "DefaultTimeout wrong value")
	assert.NotEqualValues(t, 0, config.Manager.WokersCount, "WokersCount wrong value")
	assert.NotEqualValues(t, 0, config.Manager.ReqChannelSize, "ReqChannelSize wrong value")
	assert.NotEqualValues(t, 0, int(config.Manager.PartitionConcurrency), "PartitionConcurrency wrong value")
	assert.NotEqualValues(t, 0, config.Manager.Load, "PartitionConcurrency wrong value")
	assert.NotEqualValues(t, 0, config.Manager.PartitionReplicas, "PartitionReplicas wrong value")
	assert.NotEqualValues(t, 0, config.Manager.RingDebounce, "RingDebounce wrong value")
	assert.EqualValues(t, false, config.Manager.Operator, "Operator wrong value")

	// consensus config
	assert.NotEqualValues(t, 0, config.Consensus.EpochTime, "EpochTime wrong value")
	assert.EqualValues(t, "/data/raft", config.Consensus.DataPath, "DataPath wrong value")
	assert.EqualValues(t, false, config.Consensus.EnableLogs, "EnableLogs wrong value")
	assert.EqualValues(t, true, config.Consensus.AutoBootstrap, "AutoBootstrap wrong value")
	assert.NotEqualValues(t, 0, config.Consensus.BootstrapTimeout, "BootstrapTimeout wrong value")

	// gossip config
	assert.EqualValues(t, []string{"store:8081", "store-0:8081", "store-0.store.default:8081"}, config.Gossip.InitMembers, "InitMembers wrong value")
	assert.EqualValues(t, false, config.Gossip.EnableLogs, "EnableLogs wrong value")

	// storage
	assert.EqualValues(t, "/data/storage", config.Storage.DataPath, "DataPath wrong value")

	// http
	assert.NotEqual(t, 0, config.Http.DefaultTimeout, "DefaultTimeout wrong value")
}

func TestConfigOverwrite(t *testing.T) {
	CopyFile("test-config.yaml", "config.yaml")
	defer DeleteFile("config.yaml")

	config := GetConfig()

	// manager config
	assert.EqualValues(t, 9, config.Manager.ReplicaCount, "ReplicaCount wrong value")
	assert.EqualValues(t, 99, config.Manager.WriteQuorum, "WriteQuorum wrong value")
	assert.EqualValues(t, 999, config.Manager.ReadQuorum, "ReadQuorum wrong value")
	assert.EqualValues(t, 99999, config.Manager.PartitionBuckets, "PartitionBuckets wrong value")
	assert.EqualValues(t, "/tmp/store/data", config.Manager.DataPath, "DataPath wrong value")
	assert.EqualValues(t, 991, config.Manager.PartitionConcurrency, "PartitionConcurrency wrong value")
	assert.EqualValues(t, true, config.Manager.ThreadRequests, "ThreadRequests wrong value")

	// consensus config
	assert.EqualValues(t, 9, config.Consensus.EpochTime, "EpochTime wrong value")
	assert.EqualValues(t, "/tmp/store/raft", config.Consensus.DataPath, "DataPath wrong value")
	assert.EqualValues(t, true, config.Consensus.EnableLogs, "EnableLogs wrong value")
	assert.EqualValues(t, false, config.Consensus.AutoBootstrap, "AutoBootstrap wrong value")
	assert.EqualValues(t, 1, config.Consensus.BootstrapTimeout, "BootstrapTimeout wrong value")

	// memberlist config
	assert.EqualValues(t, []string{"test:1", "test:2"}, config.Gossip.InitMembers, "InitMembers wrong value")

	assert.EqualValues(t, "/tmp/store/data", config.Storage.DataPath, "DataPath wrong value")

	assert.EqualValues(t, 33, config.Http.DefaultTimeout, "Http.DefaultTimeout wrong value")
}

func TestErrors(t *testing.T) {
	return
	errRoot := eris.New("root error")

	errOther := eris.New("root error")
	logrus.Errorf("1: %+v", fmt.Errorf("testing.. %s", "ok"))

	logrus.Errorf("2: %+v", errors.New("test"))

	// logrus.Errorf("1: %+v", fmt.Errorf("testing.. %s", "ok"))

	err := eris.Wrap(errRoot, "additional context")

	// add more context to the error
	err = eris.Wrap(err, "more context")

	// print the error with the stack trace
	// fmt.Printf("%v\n", err)
	logrus.Errorf("Error with stack trace: %+v", err)
	logrus.Error()

	logrus.Errorf("%+v", eris.Wrap(errRoot, "more context"))

	if errors.Is(err, errOther) {
		logrus.Error("The error is originally errRoot")
	} else {
		logrus.Error("The error is not originally errRoot")
	}

	if errors.Is(err, errRoot) {
		logrus.Error("The error is originally errRoot")
	} else {
		logrus.Error("The error is not originally errRoot")
	}
}

func CopyFile(src, dst string) error {
	// open the source file
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// create the destination file
	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	// copy the contents of the source file to the destination file
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return err
	}

	// sync the destination file to ensure all data is written to disk
	err = dstFile.Sync()
	if err != nil {
		return err
	}

	return nil
}

// DeleteFile deletes a file
func DeleteFile(filename string) error {
	// delete the file
	err := os.Remove(filename)
	if err != nil {
		return err
	}

	return nil
}
