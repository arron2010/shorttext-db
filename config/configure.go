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

func LoadSettings(cpath string, f func(config *Config)) {
	once.Do(func() {

		casePath := flag.String("cpath", "/opt/gopath/bin/case.txt", "定义服务器集群信息")
		flag.Parse()
		if !flag.Parsed() {
			panic("程序参数解析失败")
		}
		configInfo = &Config{}
		bytes, err := ioutil.ReadFile(configPath)
		//logger.Infof("服务器配置信息：%s\n", string(bytes))

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
		if f != nil {
			f(configInfo)
		}
	})
}

//KV数据库节点代理服务器简称代理服务器
type Config struct {
	//集群服务器配置文件路径
	KVCasePath string `json:"KVCasePath"`

	//代理服务器地址
	KVServerAddr string `json:"KVServerAddr"`

	//代理服务器GRPC连接和调用超时时长
	KVTimeout int64 `json:"KVTimeout"`

	//代理服务器GRPC连接空闲时间
	KVIdleTimeout int64 `json:"KVIdleTimeout"`

	//KV数据库文件路径
	KVDBFilePath string `json:"KVDBFilePath"`

	//KV数据库名字
	KVDBNames []string `json:"KVDBNames"`

	//同一个KV数据库的分库数量
	KVDBMaxRange int64 `json:"KVDBMaxRange"`

	//KV数据节点存储记录数量
	KVDBRowCount int64 `json:"KVDBRowCount"`

	//KV数据节点范围分区起始值
	KVDRowStart int64 `json:"KVDRowStart"`

	//序列服务器地址
	SequenceServer string `json:"SequenceServer"`

	DictPath        string `json:"DictPath"`
	HmmPath         string `json:"HmmPath"`
	UserDictPath    string `json:"UserDictPath"`
	IdfPath         string `json:"IdfPath"`
	StopWordsPath   string `json:"StopWordsPath"`
	WorkerPerMaster int
}

func GetConfig() *Config {

	return configInfo
}
func GetCase() *Case {
	return caseInfo
}
