package tailfile

import (
	"github.com/logagent/common"
	"github.com/sirupsen/logrus"
)

// tailTask 的管理者
type tailTaskMgr struct {
	// 所有的tailTask任务
	tailTaskMap map[string]*tailTask
	// 所有的配置项
	collectEntryList []common.CollectEntry
	// 等待新配置的通道
	confChan chan []common.CollectEntry
}

var ttMgr *tailTaskMgr

// Init 初始化tail,在main函数调用
func Init(allConf []common.CollectEntry) (err error) {
	// 初始化 tailTaskMgr结构体
	ttMgr = &tailTaskMgr{
		tailTaskMap:      make(map[string]*tailTask, 20),
		collectEntryList: allConf,
		confChan:         make(chan []common.CollectEntry),
	}

	// allConf里面存了若干个日志的收集项
	// 针对每一个日志收集项应该创建一个对应TailObj
	// 遍历allConf，
	for _, conf := range allConf {
		// 一个配置项对应一个构造函数 即创建一个日志收集的任务
		tt := newTailTask(conf.Path, conf.Topic)
		// 初始化tail的方法 即去打开日志文件准备读
		err = tt.Init()
		if err != nil {
			logrus.Errorf("create tailObj for path:%s failed, err:%v\n", conf.Path, err)
			continue // 启动错误 就可启动下一个
		}
		// 创建一个日志收集任务，打印一条提示信息
		logrus.Infof("create a tail task for path:%s success", conf.Path)

		// 把创建的tailTask任务登记一下，方便后续管理
		ttMgr.tailTaskMap[tt.path] = tt // 即使用path字段作为map的key

		// 可启动一个后台的goroutine，去收集日志
		go tt.run()
	}
	// 调用方法 在后台等新的配置来
	go ttMgr.watch()
	return
}

func (t *tailTaskMgr) watch() {
	// 循环的等待etcd配置项的变化
	for {
		// 等着新的配置来 新的配置来以后应该管理一些之前启动的那些tailTask
		// 一直从通道中取值
		newConf := <-t.confChan // 能取到值说明新的配置来了
		logrus.Infof("get new conf from etcd conf:%v", newConf)
		// 管理tailTask
		for _, conf := range newConf {
			// 1. 原来已经存在的tailTask任务就不用动
			// 调用方法 如果存在就跳过
			if t.isExist(conf) {
				continue
			}
			// 2. 原来没有的要新创建tailTask任务
			tt := newTailTask(conf.Path, conf.Topic)
			err := tt.Init()
			if err != nil {
				logrus.Errorf("create tailObj for path:%s failed, err:%v\n", conf.Path, err)
				continue // 启动错误 就可启动下一个
			}
			logrus.Infof("create a tail task for path:%s success", conf.Path)
			// 然后在记录一下存放到map中
			t.tailTaskMap[tt.path] = tt
			go tt.run() // 然后在后台运行
		}
		// 3. 原来有的现在没有，就要停掉tailTask任务
		// 	找到当前tailTaskMap中存在，但是在newConf(即新配置项)不存在的tailTask，把他们停掉
		// 如当前有1,2,3任务，新的配置有2,3任务，就找到1号任务挺掉
		for key, task := range t.tailTaskMap {
			var found bool
			for _, conf := range newConf {
				if key == conf.Path {
					found = true
					break
				}
			}
			if !found { // 挺掉这个任务
				logrus.Infof("这个任务 path:%s 需要停止...", task.path)
				// 删除map中key对应的任务
				delete(t.tailTaskMap, key)
				task.cancel()
			}
		}

	}
}

// 编写方法判断原来是否有tailTask任务
func (t *tailTaskMgr) isExist(conf common.CollectEntry) bool {
	// 去map中取值(即对应的key,就是path字段) 有就是true，否则false
	_, ok := t.tailTaskMap[conf.Path]
	return ok
}

// SendNesConf 把新管道变量暴露出去
func SendNesConf(newConf []common.CollectEntry) {
	ttMgr.confChan <- newConf
}
