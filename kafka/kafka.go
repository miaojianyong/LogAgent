package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)
// kafka相关

// 定义全局的 client即可启动多个kafka
var(
	client sarama.SyncProducer // 外部包没有使用就尽量用小写
	// MsgChan 因为向kafka发送的信息类型 就是sarama.ProducerMessage
	msgChan chan *sarama.ProducerMessage // 使用地址占用内存也小
)
// Init 初始化全局的kafka 连接 即Client 传递连接kafka地址
func Init(address []string,chanSize int64) (err error) {
	// 1. 生产者配置
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true

	// 2. 连接kafka
	client, err = sarama.NewSyncProducer(address,config)
	if err != nil {
		logrus.Error("producer closed, err:",err)
		return
	}
	// 初始化MsgChan 大小是参数，在ini文件可以配置
	msgChan = make(chan *sarama.ProducerMessage,chanSize)
	// 起一个后台的goroutine，从MsgChan中读取数据
	go sendMeg()
	return
}

// 从MsgChan中读取msg，发送给kafka
func sendMeg() {
	for { // 循环从管道读取数据
		select {
		case msg := <- msgChan:
			// 把读取的数据发送给kafka
			pid,offset,err := client.SendMessage(msg)
			if err != nil {
				logrus.Warning("send msg failed,err:",err)
				return
			}
			// 发送成功 在终端打印提示信息
			logrus.Infof("send msg to kafka success, pid:%v offset:%v",pid,offset)
		}
	}
}

// MsgChan 防止别人向msgChan中读取数据 故设置为消息，但是其他包会用到定义函数暴露出去
func ToMsgChan(msg *sarama.ProducerMessage) {
	msgChan <-msg
}
//// 或者暴露出去一个只写的管道
//func MsgChan2() chan<- *sarama.ProducerMessage {
//	return msgChan
//}