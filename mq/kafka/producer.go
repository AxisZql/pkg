package kafka

import (
	"github.com/AxisZql/pkg/errors"
	"github.com/Shopify/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"log"
	"os"
	"sync"
	"time"
)

/*
@Author: AxisZql
@Desc: encapsulation kafka producer
@Date: 2022 Oct 15 10:25 PM
*/

type Producer struct {
	Name       string
	Hosts      []string
	Config     *sarama.Config
	Status     string
	Breaker    *breaker.Breaker
	ReConnect  chan bool
	StatusLock sync.Mutex
}

// Msg kafka 发送消息的结构体
type Msg struct {
	Topic     string
	KeyBytes  []byte
	DataBytes []byte
}

// SyncProducer synchronization producer
type SyncProducer struct {
	Producer
	SyncProducer *sarama.SyncProducer
}

// AsyncProducer asynchronization producer
type AsyncProducer struct {
	Producer
	AsyncProducer *sarama.AsyncProducer
}

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

const (
	ProducerConnected    string = "connected"
	ProducerDisConnected string = "disconnected"
	ProducerClosed       string = "closed"

	DefaultKafkaAsyncProducer = "default-kafka-async-producer"
	DefaultKafkaSyncProducer  = "default-kafka-sync-producer"
)

var (
	ErrProducerTimeout = errors.New("push message timeout")
	SyncProducers      = make(map[string]*SyncProducer)
	AsyncProducers     = make(map[string]*AsyncProducer)
	StdLogger          stdLogger
)

func init() {
	StdLogger = log.New(os.Stdout, "[kafka]", log.LstdFlags|log.Lshortfile)
}

func MsgValueEncoder(value []byte) sarama.Encoder {
	return sarama.ByteEncoder(value)
}

func MsgValueStrEncoder(value string) sarama.Encoder {
	return sarama.ByteEncoder(value)
}

// set default producer
func getDefaultProducerConfig(clientID string) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V2_0_0_0

	config.Net.DialTimeout = time.Second * 30
	config.Net.WriteTimeout = time.Second * 30
	config.Net.ReadTimeout = time.Second * 30

	// 重试时间间隔
	config.Producer.Retry.Backoff = time.Millisecond * 500
	config.Producer.Retry.Max = 3

	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	// 最大消息限制：需要小于broker的：message.max.bytes配置，默认是1000000
	config.Producer.MaxMessageBytes = 1000000 * 2

	// acks == 1表示leader写入即算成功
	config.Producer.RequiredAcks = sarama.WaitForLocal
	// 利用hash分区来确保当对应topic当partition数不为0的话，也可以保证消息的时序性
	config.Producer.Partitioner = sarama.NewHashPartitioner

	// zstd 算法有最高压缩比，而在吞吐量上的表现一般：LZ4>Snappy>zstd 和 GZIP
	// 压缩比：zstd>LZ4>GZIP>Snappy
	// 故最优选择LZ4
	config.Producer.Compression = sarama.CompressionLZ4
	return
}

// InitSyncProducer init synchronization kafka producer
func InitSyncProducer(name string, hosts []string, config *sarama.Config) error {
	syncProducer := &SyncProducer{}
	syncProducer.Name = name
	syncProducer.Hosts = hosts
	syncProducer.Status = ProducerDisConnected

	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	syncProducer.Config = config
	if producer, err := sarama.NewSyncProducer(hosts, config); err != nil {
		return errors.Warpf(err, "NewSyncProducer error name :%s", name)
	} else {
		// 即熔断限流：如果超过错误阈值errorThreshold时，而又没有一个至少timeout的无错期时则开启断路器「circuit-breaker」
		// 在timeout后，断路器从open变为half-open，从half-open到在经过successThreshold「成功阈值」后，断路器关闭
		syncProducer.Breaker = breaker.New(3, 1, 2*time.Second)
		syncProducer.ReConnect = make(chan bool)
		syncProducer.SyncProducer = &producer
		syncProducer.Status = ProducerConnected

	}
	return nil
}
