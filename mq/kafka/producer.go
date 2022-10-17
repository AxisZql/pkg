package kafka

import (
	"fmt"
	"github.com/AxisZql/pkg/xlog"
	"github.com/Shopify/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/pkg/errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
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

//===============================Synchronization Producer=========================

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
		return errors.Wrapf(err, "NewSyncProducer error name :%s", name)
	} else {
		// 即熔断限流：如果超过错误阈值errorThreshold时，而又没有一个至少timeout的无错期时则开启断路器「circuit-breaker」
		// 在timeout后，断路器从open变为half-open，从half-open到在经过successThreshold「成功阈值」后，断路器关闭
		syncProducer.Breaker = breaker.New(3, 1, 2*time.Second)
		syncProducer.ReConnect = make(chan bool)
		syncProducer.SyncProducer = &producer
		syncProducer.Status = ProducerConnected
		xlog.Info(fmt.Sprintf("SyncProducer connected name:%s", name))
	}
	go syncProducer.keepConnect() // keepConnect
	go syncProducer.check()       // check
	SyncProducers[name] = syncProducer
	return nil
}

func GetSyncProducer(name string) *SyncProducer {
	if producer, ok := SyncProducers[name]; ok {
		return producer
	} else {
		StdLogger.Println("InitSyncKafkaProducer must be called!")
		return nil
	}
}

// SendMessages send batch messages to kafka synchronously
func (syncProducer *SyncProducer) SendMessages(mses []*sarama.ProducerMessage) (errs sarama.ProducerErrors) {
	if syncProducer.Status != ProducerConnected {
		return append(errs, &sarama.ProducerError{Err: errors.New(fmt.Sprintf("kafka syncProducer" + syncProducer.Status))})
	}
	errs = (*syncProducer.SyncProducer).SendMessages(mses).(sarama.ProducerErrors)
	for _, err := range errs {
		// 触发重连
		if errors.Is(err, sarama.ErrBrokerNotAvailable) {
			syncProducer.StatusLock.Lock()
			if syncProducer.Status == ProducerConnected {
				syncProducer.Status = ProducerDisConnected
				syncProducer.ReConnect <- true
			}
			syncProducer.StatusLock.Unlock()
		}
	}
	return
}

// Send  message to kafka synchronously
func (syncProducer *SyncProducer) Send(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if syncProducer.Status != ProducerConnected {
		return -1, -1, errors.New(fmt.Sprintf("kafka syncProducer %s", syncProducer.Status))
	}
	partition, offset, err = (*syncProducer.SyncProducer).SendMessage(msg)
	if err == nil {
		return
	}
	if errors.Is(err, sarama.ErrBrokerNotAvailable) {
		syncProducer.StatusLock.Lock()
		if syncProducer.Status == ProducerConnected {
			syncProducer.Status = ProducerDisConnected
			syncProducer.ReConnect <- true
		}
		syncProducer.StatusLock.Unlock()
	}
	return
}

func (syncProducer *SyncProducer) Close() error {
	syncProducer.StatusLock.Lock()
	defer syncProducer.StatusLock.Unlock()
	err := (*syncProducer.SyncProducer).Close()
	syncProducer.Status = ProducerClosed
	return err
}

// keepConnect continuously check the online status of the producer to ensure that
// the producer is always online
func (syncProducer *SyncProducer) keepConnect() {
	defer func() {
		StdLogger.Println("syncProducer keepConnect exited")
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if syncProducer.Status == ProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			syncProducer.Status = ProducerClosed
			syncProducer.StatusLock.Unlock()
			return
		case <-syncProducer.ReConnect:
			if syncProducer.Status != ProducerDisConnected {
				break
			}
			StdLogger.Println(fmt.Sprintf("kafka syncProducer ReConnecting...name:%s", syncProducer.Name))
			var producer sarama.SyncProducer
		syncBreakLoop:
			for {
				// 利用熔断器给集群以恢复时间，避免不断的发送重连请求
				err := syncProducer.Breaker.Run(func() (err error) {
					producer, err = sarama.NewSyncProducer(syncProducer.Hosts, syncProducer.Config)
					return
				})
				switch err {
				case nil:
					syncProducer.StatusLock.Lock()
					if syncProducer.Status == ProducerDisConnected {
						syncProducer.SyncProducer = &producer
						syncProducer.Status = ProducerConnected
					}
					syncProducer.StatusLock.Unlock()
					StdLogger.Println(fmt.Sprintf("kafka syncProuducer ReConnected,name:%s", syncProducer.Name))
					break syncBreakLoop
				case breaker.ErrBreakerOpen:
					StdLogger.Println("kafka connect fail,broker is open")
					// 2s后重连，此时breaker刚好half-close
					if syncProducer.Status == ProducerDisConnected {
						time.AfterFunc(2*time.Second, func() {
							StdLogger.Println("kafka begin to ReConnect,because of ErrBreakerOpen")
							syncProducer.ReConnect <- true
						})
					}
					break syncBreakLoop
				default:
					StdLogger.Println(fmt.Sprintf("kafka ReConnect err,name:%s", syncProducer.Name))
				}
			}
		}
	}
}

// check the producer status
func (syncProducer *SyncProducer) check() {
	defer func() {
		StdLogger.Println("syncProducer check exited")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if syncProducer.Status == ProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			syncProducer.Status = ProducerDisConnected
			syncProducer.StatusLock.Unlock()
			return
		}
	}
}

//===============================Asynchronization Producer=========================

// InitAsyncProducer init async kafka producer
func InitAsyncProducer(name string, hosts []string, config *sarama.Config) error {
	asyncProducer := &AsyncProducer{}
	asyncProducer.Name = name
	asyncProducer.Hosts = hosts
	asyncProducer.Status = ProducerDisConnected
	if config == nil {
		config = getDefaultProducerConfig(name)
	}
	asyncProducer.Config = config
	if producer, err := sarama.NewAsyncProducer(hosts, config); err != nil {
		return errors.Wrapf(err, "NewAsyncProducer error name:%s", name)
	} else {
		asyncProducer.Breaker = breaker.New(3, 1, 5*time.Second)
		asyncProducer.ReConnect = make(chan bool)
		asyncProducer.AsyncProducer = &producer
		asyncProducer.Status = ProducerConnected
		StdLogger.Println(fmt.Sprintf("AsyncProducer connected name:%s", name))
	}
	go asyncProducer.keepConnect()
	go asyncProducer.check()
	AsyncProducers[name] = asyncProducer
	return nil
}

func GetAsyncProducer(name string) *AsyncProducer {
	if producer, ok := AsyncProducers[name]; ok {
		return producer
	} else {
		StdLogger.Println("InitAsyncProducer must be called")
		return nil
	}
}

func (asyncProducer *AsyncProducer) Send(msg *sarama.ProducerMessage) error {
	var err error
	if asyncProducer.Status != ProducerConnected {
		return errors.New("kafka disconnected")
	}
	(*asyncProducer.AsyncProducer).Input() <- msg
	return err
}

func (asyncProducer *AsyncProducer) Close() error {
	asyncProducer.StatusLock.Lock()
	defer asyncProducer.StatusLock.Unlock()
	err := (*asyncProducer.AsyncProducer).Close()
	asyncProducer.Status = ProducerClosed
	return err
}

// keepConnect continuously check the online status of the asyncProducer to ensure that
// the producer is always online
func (asyncProducer *AsyncProducer) keepConnect() {
	defer func() {
		StdLogger.Println("asyncProducer keepConnect exited")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if asyncProducer.Status == ProducerClosed {
			return
		}
		select {
		case s := <-signals:
			StdLogger.Println(fmt.Sprintf("kafka async producer receive system signal:%s,name:%s", s.String(), asyncProducer.Name))
			// todo: leave the closing to the application layer
			asyncProducer.StatusLock.Lock()
			asyncProducer.Status = ProducerClosed
			asyncProducer.StatusLock.Unlock()
			return
		case <-asyncProducer.ReConnect:
			if asyncProducer.Status != ProducerDisConnected {
				break
			}
			StdLogger.Println(fmt.Sprintf("kafka syncProducer ReConnecting...;name:%s", asyncProducer.Name))
			var producer sarama.AsyncProducer
		asyncBreakLoop:
			for {
				// 利用熔断器给集群以恢复时间，避免不必要的重连
				err := asyncProducer.Breaker.Run(func() (err error) {
					producer, err = sarama.NewAsyncProducer(asyncProducer.Hosts, asyncProducer.Config)
					return
				})
				switch err {
				case nil:
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == ProducerDisConnected {
						asyncProducer.AsyncProducer = &producer
						asyncProducer.Status = ProducerConnected
					}
					asyncProducer.StatusLock.Unlock()
					StdLogger.Println(fmt.Sprintf("kafka asyncProducer Reconnected,name:%s", asyncProducer.Name))
					break asyncBreakLoop
				case breaker.ErrBreakerOpen:
					StdLogger.Println("kafka connect fail,breaker is open")
					// 5s后重连，此时breaker刚好half-close
					if asyncProducer.Status == ProducerDisConnected {
						time.AfterFunc(5*time.Second, func() {
							StdLogger.Println("kafka begin to ReConnect,because of ErrBreakerOpen")
							asyncProducer.ReConnect <- true
						})
					}
					break asyncBreakLoop
				default:
					StdLogger.Println(fmt.Sprintf("kafka ReConnect error,name:%s,%v", asyncProducer.Name, err))
				}
			}
		}
	}
}

// check asyncProducer online status
func (asyncProducer *AsyncProducer) check() {
	defer func() {
		StdLogger.Println("asyncProducer check exited")
	}()
	for {
		switch asyncProducer.Status {
		case ProducerDisConnected:
			time.Sleep(time.Second * 5)
			continue
		case ProducerClosed:
			return
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
		for {
			select {
			case msg := <-(*asyncProducer.AsyncProducer).Successes():
				StdLogger.Println(fmt.Sprintf("Success producer message:%v", msg))
			case err := <-(*asyncProducer.AsyncProducer).Errors():
				StdLogger.Println(fmt.Sprintf("message send error:%v", err))
				if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
					// 连接中断触发重连，捕捉不到EOF
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == ProducerConnected {
						asyncProducer.Status = ProducerDisConnected
						asyncProducer.ReConnect <- true
					}
					asyncProducer.StatusLock.Unlock()
				}
			case s := <-signals:
				StdLogger.Println(fmt.Sprintf("kafka async producer receive system signal:%s,name:%s", s.String(), asyncProducer.Name))
				asyncProducer.StatusLock.Lock()
				asyncProducer.Status = ProducerClosed
				asyncProducer.StatusLock.Unlock()
				return
			}
		}
	}
}
