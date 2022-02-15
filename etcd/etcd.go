package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/logagent/common"
	"github.com/logagent/tailfile"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"time"
)

// etcd相关操作
var (
	client *clientv3.Client
)

func Init(address []string) (err error) {
	client, err = clientv3.New(clientv3.Config{
		Endpoints:   address,
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		fmt.Println("connect to etcd failed, err:", err)
		return
	}
	return
}

// GetConf 拉取日志收集项的函数 返回值切片类型collectEntry的结构体
func GetConf(key string) (collectEntryList []common.CollectEntry, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := client.Get(ctx, key)
	if err != nil {
		logrus.Errorf("get conf from etcd by key:%s failed, err:%v", key, err)
		return
	}
	// 如果读取的配置项是0长度就报错
	if len(resp.Kvs) == 0 {
		logrus.Warningf("get len:0 conf from etcd by key:%s", key)
		return
	}
	ret := resp.Kvs[0] // 取出第1个
	// ret.Value 是要JSON格式的字符串 故反序列化解析
	err = json.Unmarshal(ret.Value, &collectEntryList)
	if err != nil {
		logrus.Errorf("json unmarshal failed, err:%v", err)
		return
	}
	return
}

// WatchConf 监控etcd中日志收集项配置变化
func WatchConf(key string) {
	// 循环监听 etcd配置文件的变化
	for {
		watchCh := client.Watch(context.Background(), key)

		for wresp := range watchCh {
			logrus.Info("get new conf from etcd!") // 获取到新的配置
			for _, evt := range wresp.Events {
				fmt.Printf("type:%s key:%s value:%s\n",
					evt.Type, evt.Kv.Key, evt.Kv.Value)
				// 定义新的配置项变量
				var newConf []common.CollectEntry
				// 如果是删除配置项事件 就跳过 即传递一个空的newConf
				if evt.Type == clientv3.EventTypeDelete {
					logrus.Warning("etcd delete the key!!!")
					tailfile.SendNesConf(newConf)
					continue
				}
				// 反序列化配置项数据
				err := json.Unmarshal(evt.Kv.Value, &newConf)
				if err != nil {
					logrus.Errorf("json unmarshal new conf failed, err:%v", err)
					continue // 解析失败 就跳过
				}
				// 告诉tailfile这个模块 应该启用新的配置了
				tailfile.SendNesConf(newConf) // 没有接收 就是阻塞了
			}
		}
	}
}
