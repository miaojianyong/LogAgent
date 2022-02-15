package common

import (
	"net"
)

// CollectEntry 要是收集日志配置项的结构体
type CollectEntry struct {
	Path  string `json:"path"`  // 去哪个路径读取日志文件
	Topic string `json:"topic"` // 日志文件发往哪个kafka中的哪个topic
}

// GetOutboundIP 获取本机ip
func GetOutboundIP() (ip string, err error) {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip = localAddr.IP.String()
	return
}
