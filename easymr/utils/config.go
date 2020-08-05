package utils

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
)

type Config struct {
	ClusterPath         string `json:"ClusterPath"`
	ModelPath           string `json:"ModelPath"`
	DictPath            string `json:"DictPath"`
	HmmPath             string `json:"HmmPath"`
	UserDictPath        string `json:"UserDictPath"`
	IdfPath             string `json:"IdfPath"`
	StopWordsPath       string `json:"StopWordsPath"`
	LogPath             string `json:"LogPath"`
	WorkerPerMaster     int    `json:"WorkerPerMaster"`
	ServerNum           int    `json:"ServerNum"`
	MaxTaskCount        int    `json:"MaxTaskCount"`
	CacheServerAddr     string `json:"CacheServerAddr"`     //redis服务器地址
	CacheServerPassword string `json:"CacheServerPassword"` //redis服务器访问密码
	SensitiveDicPath    string `json:"SensitiveDicPath"`    //敏感词汇表，用于特殊字符转义
	MaxLeftLength       int    `json:"MaxLeftLength"`
	MaxRightLength      int    `json:"MaxRightLength"`
	MaxLeftLength2      int    `json:"MaxLeftLength2"`
	MaxRightLength2     int    `json:"MaxRightLength2"`
	OriginalDataFile    string `json:"OriginalDataFile"`
}

var once sync.Once
var settings *Config

func GetSettings() *Config {

	once.Do(func() {

		configPath := os.Getenv("MR_CUSTOM_CONFIG")
		if configPath == "" {
			configPath = "/opt/gopath/bin/mr_config.json"
		}
		settings = new(Config)
		bytes, err := ioutil.ReadFile(configPath)
		if err != nil {
			panic(err)
		}

		if err := json.Unmarshal(bytes, settings); err != nil {
			panic(err)
		}
	})
	return settings
}
