package config

import (
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type ManagerConfig struct {
	WokersCount          int  `mapstructure:"WORKERS_COUNT"`
	ThreadRequests       bool `mapstructure:"THREAD_REQUESTS"`
	ReqChannelSize       int  `mapstructure:"REQ_CHANNEL_SIZE"`
	ReplicaCount         int  `mapstructure:"REPLICA_COUNT"`
	WriteQuorum          int  `mapstructure:"WRITE_QUORUM"`
	ReadQuorum           int  `mapstructure:"READ_QUORUM"`
	SaveInterval         int
	DefaultTimeout       int     `mapstructure:"DEFAULT_TIMEOUT"`
	PartitionCount       int     `mapstructure:"PARTITION_COUNT"`
	Load                 float64 `mapstructure:"LOAD"`
	PartitionBuckets     int     `mapstructure:"PARTITION_BUCKETS"`
	PartitionReplicas    int     `mapstructure:"PARTITION_REPLICAS"`
	PartitionConcurrency int     `mapstructure:"PARTITION_CONCURRENCY"`
	DataPath             string  `mapstructure:"DATA_PATH"` // TODO REMOVE THIS?
	Hostname             string
	RingDebounce         float64 `mapstructure:"RING_DEBOUNCE"`
	Operator             bool
}

type ConsensusConfig struct {
	DataPath         string `mapstructure:"DATA_PATH"`
	EpochTime        int    `mapstructure:"EPOCH_TIME"`
	EnableLogs       bool   `mapstructure:"ENABLE_LOGS"`
	AutoBootstrap    bool   `mapstructure:"AUTO_BOOTSTRAP"`
	BootstrapTimeout int    `mapstructure:"BOOTSTRAP_TIMEOUT"`
	Name             string
}

type GossipConfig struct {
	InitMembers []string `mapstructure:"INIT_MEMBERS"`
	Name        string
	EnableLogs  bool `mapstructure:"ENABLE_LOGS"`
}

type StorageConfig struct {
	DataPath string `mapstructure:"DATA_PATH"`
}

type RpcConfig struct {
	Port           int `mapstructure:"PORT"`
	DefaultTimeout int `mapstructure:"DEFAULT_TIMEOUT"`
}
type HttpConfig struct {
	DefaultTimeout int `mapstructure:"DEFAULT_TIMEOUT"`
	Hostname       string
}

type Config struct {
	Manager   ManagerConfig   `mapstructure:"MANAGER"`
	Consensus ConsensusConfig `mapstructure:"CONSENSUS"`
	Gossip    GossipConfig    `mapstructure:"GOSSIP"`
	Storage   StorageConfig   `mapstructure:"STORAGE"`
	Rpc       RpcConfig       `mapstructure:"RPC"`
	Http      HttpConfig      `mapstructure:"HTTP"`
}

func GetConfig() Config {
	return getConfigOverride(true)
}

func GetDefaultConfig() Config {
	return getConfigOverride(false)
}

func getConfigOverride(allow_override bool) Config {
	logrus.Debugf("CONFIG_PATH = ", os.Getenv("CONFIG_PATH"))
	viper.AddConfigPath(os.Getenv("CONFIG_PATH"))

	viper.SetConfigName("default-config")
	if err := viper.ReadInConfig(); err != nil {
		logrus.Fatalf("Error reading default config file, %s", err)
	}

	// Override with configuration from config.yaml
	if allow_override {
		viper.SetConfigName("config")
		if err := viper.MergeInConfig(); err != nil {
			logrus.Debugf("Error reading user defined config file, %s", err)
		}
	}

	var config Config
	err := viper.Unmarshal(&config)
	if err != nil {
		logrus.Fatalf("unable to decode into struct, %v", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		logrus.Fatal(err)
	}
	config.Manager.Hostname = hostname
	config.Gossip.Name = hostname
	config.Consensus.Name = hostname
	config.Http.Hostname = hostname

	_, exists := os.LookupEnv("OPERATOR")
	config.Manager.Operator = exists

	// // Print the JSON string
	// settings := viper.AllSettings()
	// jsonString, err := json.MarshalIndent(settings, "", "  ")
	// if err != nil {
	// 	logrus.Fatal(err)
	// }
	// fmt.Println(string(jsonString))

	return config
}
