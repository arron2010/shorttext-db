package config

import (
	"encoding/json"
	"flag"
	"github.com/xp/shorttext-db/glogger"
	"io/ioutil"
	"sync"
)

var caseInfo *Case
var once sync.Once
var configInfo *Config

const configPath = "/opt/gopath/bin/db_config.txt"

var logger = glogger.MustGetLogger("config")

func LoadSettings(cpath string) {
	once.Do(func() {

		casePath := flag.String("cpath", "/opt/gopath/bin/case.txt", "定义服务器集群信息")
		flag.Parse()
		if !flag.Parsed() {
			panic("程序参数解析失败")
		}
		configInfo = &Config{}
		bytes, err := ioutil.ReadFile(configPath)
		logger.Infof("服务器配置信息：%s\n", string(bytes))

		if err != nil {
			panic(err)
		}
		if err = json.Unmarshal(bytes, configInfo); err != nil {
			panic("服务配置加载异常")
		}
		if len(cpath) > 0 {
			configInfo.KVCasePath = cpath
		} else {
			configInfo.KVCasePath = *casePath
		}

		caseInfo = &Case{}
		bytes, err = ioutil.ReadFile(configInfo.KVCasePath)
		if err = json.Unmarshal(bytes, caseInfo); err != nil {
			panic("集群配置加载异常")
		}

	})
}

//KV数据库节点代理服务器简称代理服务器
type Config struct {
	//集群服务器配置文件路径
	KVCasePath string

	//代理服务器地址
	KVServerAddr string

	//代理服务器GRPC连接和调用超时时长
	KVTimeout int64

	//代理服务器GRPC连接空闲时间
	KVIdleTimeout int64

	//KV数据库文件路径
	KVDBFilePath string

	//KV数据库名字
	KVDBNames []string

	//同一个KV数据库的分库数量
	KVDBMaxRange int64

	//KV数据节点存储记录数量
	KVDBRowCount int64

	//KV数据节点范围分区起始值
	KVDRowStart int64

	//序列服务器地址
	SequenceServer string
}

func GetConfig() *Config {

	return configInfo
}
func GetCase() *Case {
	return caseInfo
}
