package tailfile

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/hpcloud/tail"
	"github.com/logagent/kafka"
	"github.com/sirupsen/logrus"
	"strings"
	"time"
)

// tail相关

type tailTask struct {
	path  string
	topic string
	tObj  *tail.Tail
	// 使用context 用来关闭goroutine
	ctx    context.Context
	cancel context.CancelFunc // 调用该函数就会关闭goroutine
}

//var (
//	TailObj *tail.Tail
//)

// 定义管道 接收配置项数据
//var (
//	confChan chan []common.CollectEntry
//)

// 也可创建上述结构体的构造函数
func newTailTask(path, topic string) *tailTask {
	// 调用context用来初始化结构体中的context相关字段
	ctx, cancel := context.WithCancel(context.Background())
	tt := &tailTask{
		path:   path,
		topic:  topic,
		ctx:    ctx,
		cancel: cancel,
	}
	return tt
}

//  tail的初始化配置方法
func (t *tailTask) Init() (err error) {
	config := tail.Config{
		// 文件到一定大小可 跟上文件并自动打开
		ReOpen: true,
		Follow: true,
		// 打开文件后 从什么地方读取数据 Whence: 2表示从文件末尾去读
		Location: &tail.SeekInfo{Offset: 0, Whence: 2},
		// 运行日志文件不存在
		MustExist: false,
		// 轮询的方式
		Poll: true,
	}
	// etcd有一个日志文件 就创建一个TailObj
	// 打开文件 开始读取数据
	t.tObj, err = tail.TailFile(t.path, config)
	return
}

func (t *tailTask) run() {
	// 打印一条提示信息，一个读取日志的任务在运行
	logrus.Infof("path:%s is running...", t.path)
	// 读取日志 发往kafka
	for {
		// 使用select去读取context，即如果有数据就停掉goroutine
		select {
		case <-t.ctx.Done(): // 只要调用t.cancel() 就会有值
			logrus.Infof("path:%s is stoping...", t.path)
			t.tObj.Cleanup()     // 清理掉当前tObj对象
			err := t.tObj.Stop() // 关闭监听文件活动
			if err != nil {
				return
			}
			return // 即停掉goroutine
		case line, ok := <-t.tObj.Lines: // 否则读取读取日志内容
			if !ok {
				logrus.Warning("tail fail close reopen,path::",
					t.path)
				time.Sleep(time.Second)
				continue
			}
			if len(strings.Trim(line.Text, "\r")) == 0 {
				continue
			}
			msg := &sarama.ProducerMessage{}
			msg.Topic = t.topic // 每一个tailObj自己的topic
			msg.Value = sarama.StringEncoder(line.Text)
			kafka.ToMsgChan(msg)
		}
	}
}
