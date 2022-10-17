package kafka

import (
	"fmt"
	"github.com/AxisZql/pkg/xlog"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

/*
@Author: AxisZql
@Desc: encapsulation kafka consumer
@Date: 2022 Oct 17 8:25 AM
*/

const (
	ConsumerConnected    string = "connected"
	ConsumerDisconnected string = "disconnected"
)

type Consumer struct {
	hosts    []string
	topics   []string
	config   *cluster.Config
	consumer *cluster.Consumer
	status   string
	groupID  string

	breaker    *breaker.Breaker
	reConnect  chan bool
	statusLock sync.Mutex
	exit       bool
}

// MessageHandler 消费者回调函数
type MessageHandler func(message *sarama.ConsumerMessage) (bool, error)

// default consumer config
func getDefaultConsumerConfig() (config *cluster.Config) {
	config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	//消费者是否存活的心跳检测，默认是10秒，对应kafka session.timeout.ms配置
	config.Consumer.Group.Session.Timeout = 20 * time.Second
	//消费者协调器心跳间隔时间，默认3s此值设置不超过group session超时时间的三分之一
	config.Consumer.Group.Heartbeat.Interval = 6 * time.Second
	//消息处理时间，超时则rebalance给其他消费者，kafka默认值为300000，配置项为max.poll.interval.ms
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond
	config.Consumer.Fetch.Default = 1024 * 1024 * 2
	//此配置是重新平衡时消费者加入group的超时时间，默认是60s
	config.Consumer.Group.Rebalance.Timeout = 60 * time.Second

	config.Version = sarama.V2_0_0_0
	return
}

func StartConsumer(hosts, topics []string, groupID string, config *cluster.Config, f MessageHandler) (*Consumer, error) {
	var err error
	if config == nil {
		config = getDefaultConsumerConfig()
	}
	consumer := &Consumer{
		hosts:   hosts,
		config:  config,
		status:  ProducerDisConnected,
		groupID: groupID,
		topics:  topics,

		// 设定timeout为5s，如果timeout内没有error，则断路器到达half-open status
		breaker:   breaker.New(3, 1, 5*time.Second),
		reConnect: make(chan bool),
		exit:      false,
	}
	if consumer.consumer, err = cluster.NewConsumer(hosts, groupID, topics, consumer.config); err != nil {
		return consumer, err
	} else {
		consumer.status = ConsumerConnected
		xlog.Info("kafka consumer stared", zap.Any(groupID, topics))
	}

	go consumer.keepConnect()
	go consumer.consumerMessage(f)
	return consumer, err
}

func (c *Consumer) Close() error {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	c.exit = true
	return c.consumer.Close()
}

// keep kafka consumer connecting
func (c *Consumer) keepConnect() {
	for !c.exit {
		select {
		case <-c.reConnect:
			if c.status != ConsumerDisconnected {
				break
			}
			xlog.Warn("kafkaConsumer reconnecting", zap.Any(c.groupID, c.topics))
			var consumer *cluster.Consumer
		breakLoop:
			for {
				err := c.breaker.Run(func() (err error) {
					consumer, err = cluster.NewConsumer(c.hosts, c.groupID, c.topics, c.config)
					return
				})
				switch err {
				case nil:
					c.statusLock.Lock()
					if c.status == ConsumerDisconnected {
						c.consumer = consumer
						c.status = ConsumerConnected
					}
					c.statusLock.Unlock()
					break breakLoop
				case breaker.ErrBreakerOpen:
					xlog.Warn("kafka consumer connect fail,broker is open")
					if c.status == ConsumerDisconnected {
						time.AfterFunc(5*time.Second, func() {
							c.reConnect <- true
						})
					}
					break breakLoop
				default:
					xlog.Error("kafka consumer connect error", zap.Error(err))
				}
			}
		}
	}
}

// consumers consume message in queues all the time
func (c *Consumer) consumerMessage(f MessageHandler) {
	for !c.exit {
		if c.status != ConsumerConnected {
			time.Sleep(5 * time.Second)
			xlog.Warn(fmt.Sprintf("kafka consumer status:%s", c.status))
			continue
		}
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		for ntf := range c.consumer.Notifications() {
			xlog.Warn("kafka consumer Rebalanced", zap.Any(c.groupID, ntf))
		}
	}()

consumerLoop:
	for !c.exit {
		select {
		case msg, ok := <-c.consumer.Messages():
			if ok {
				if commit, err := f(msg); commit {
					c.consumer.MarkOffset(msg, "")
				} else {
					if err != nil {
						xlog.Error("kafka consumer msg error", zap.Error(err))
					}
				}
			}
		case err := <-c.consumer.Errors():
			xlog.Error("kafka consumer msg error", zap.Error(err))
			if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
				c.statusLock.Lock()
				if c.status == ConsumerConnected {
					c.status = ConsumerDisconnected
					c.reConnect <- true
				}
				c.statusLock.Unlock()
			} else {
				//todo 如果不是中断消息，则表示kafka挂了，进程应该退出
				panic(fmt.Sprintf("kafka server error:%s", err.Error()))
			}
		case s := <-signals:
			xlog.Warn(fmt.Sprintf("kafka consumer receive system signal:%s", s.String()))
			c.statusLock.Lock()
			c.exit = true
			err := c.consumer.Close()
			if err != nil {
				xlog.Error("consumer.Close error", zap.Error(err))
			}
			c.statusLock.Unlock()
			break consumerLoop
		}
	}
}
