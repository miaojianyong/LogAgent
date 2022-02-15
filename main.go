package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/logagent/common"
	"github.com/logagent/etcd"
	"github.com/logagent/kafka"
	"github.com/logagent/tailfile"
	"github.com/sirupsen/logrus"
)

// 日志收集 客户端
// 类似的开源项目 filebeat
// 收集指定目录下的日志文件，发送到kafka中

// 创建结构体 读取配置文件
type Config struct {
	KafkaConfig   `ini:"kafka"`
	CollectConfig `ini:"collect"`
	EtcdConfig    `ini:"etcd"`
}
type KafkaConfig struct {
	Address  string `ini:"address"`
	ChanSize int64  `ini:"chan_size"`
}
type CollectConfig struct {
	LogFilePath string `ini:"logfile_path"`
}

type EtcdConfig struct {
	Address    string `ini:"address"`
	CollectKey string `ini:"collect_key"`
}

// run 真正的业务逻辑
//func run()(err error)  {
//	// 从TailObj -->中读取log日志 -->通过kafka的client -->发送到kafka
//	// 循环从tail的管道中读取数据
//	for {
//		line, ok := <-tailfile.TailObj.Lines
//		if !ok {
//			logrus.Warning("tail fail close reopen,fileName::",
//				tailfile.TailObj.Filename)
//			time.Sleep(time.Second)
//			continue
//		}
//		// 如果读到空行 就跳过
//		if len(strings.Trim(line.Text,"\r")) == 0 {
//			continue
//		}
//		fmt.Println(line.Text)
//		// 利用kafka中的通道 将同步的代码改为异步的
//		// 把读出来的一行日志(line)，包装成kafka里面的msg类型
//		msg := &sarama.ProducerMessage{}
//		msg.Topic = "web_log"
//		msg.Value = sarama.StringEncoder(line.Text)
//		// 存到通道中
//		//kafka.MsgChan <- msg
//		// 调用函数 发送数据
//		kafka.ToMsgChan(msg)
//	}
//	return
//}
// 方便测试一直读取数据
func run() {
	select {}
}

func main() {
	// 0. 调用函数 获取本机ip
	ip, err := common.GetOutboundIP()
	if err != nil {
		logrus.Error("get ip failed, err:", err)
		return
	}
	// 创建结构体实例 因为要改变结构体的值故用指针 即new
	var configObj = new(Config)

	// 1. 使用go-ini第三方库，读取配置文件
	// cfg就是获取到了 配置文件
	/*cfg, err := ini.Load("./conf/config.ini")
	if err != nil {
		// 把错误信息打印到控制台
		logrus.Error("load config failed err:%v",err)
		return
	}
	// 从配置文件中 获取 [...节],获取key,在跟值的类型, 就得到值了
	kafkaAddr := cfg.Section("kafka").Key("address").String()
	fmt.Println(kafkaAddr) // 127.0.0.1:9092*/
	err = ini.MapTo(configObj, "./conf/config.ini")
	if err != nil {
		logrus.Errorf("load config failed err:%v", err)
		return
	}
	fmt.Printf("%#v\n", configObj)

	// 2. 初始化连接kafka
	// 调用函数，传递连接kafka的地址
	err = kafka.Init([]string{configObj.KafkaConfig.Address},
		configObj.KafkaConfig.ChanSize)
	if err != nil {
		logrus.Errorf("init kafka failed err:%v", err)
		return
	}
	// 打印连接kafka成功
	logrus.Info("init kafka success!")

	// 在这里新增
	// 调用函数 初始化etcd连接
	err = etcd.Init([]string{configObj.EtcdConfig.Address})
	if err != nil {
		logrus.Errorf("init etcd failed err:%v", err)
		return
	}
	// 调用函数 从etcd中拉取 要收集的日志配置项
	// 用上述获取到的本机ip，替换配置文件中的%s占位符
	collectKey := fmt.Sprintf(configObj.EtcdConfig.CollectKey, ip)
	allConf, err := etcd.GetConf(collectKey)
	if err != nil {
		logrus.Errorf("get conf from etcd failed err:%v", err)
		return
	}
	fmt.Println(allConf)
	// 在后台一直监控etcd中配置项的变化
	go etcd.WatchConf(collectKey)

	// 3. 根据配置中的日志路径 初始化tail
	// 调用函数，传递log文件路径
	//err = tailfile.Init(configObj.CollectConfig.LogFilePath)
	// 把从etcd中获取的配置项传递到tail的init初始化函数中
	err = tailfile.Init(allConf)
	if err != nil {
		logrus.Errorf("init tail failed err:%v", err)
		return
	}
	// tail加载成功
	logrus.Info("init tail success!")

	// 4. 把日志通过sarama发往kafka
	// 调用函数 读取数据
	//err = run()
	//if err != nil {
	//	logrus.Error("run failed,err:",err)
	//	return
	//}
	run()
}
