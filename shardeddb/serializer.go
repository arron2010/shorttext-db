package shardeddb

import (
	"com.neep/goplatform/util"
	"encoding/binary"
	"encoding/json"
	"github.com/xp/shorttext-db/utils"
)

func serialize(source interface{}) (string, error) {
	var buff []byte
	var err error
	switch source.(type) {
	case string:
		var str string = source.(string)
		return str, nil
	case int:
		buff = make([]byte, 4)
		binary.LittleEndian.PutUint32(buff, uint32(source.(int)))
	case float32:
		var f float32 = source.(float32)
		buff = utils.Float32ToByte(f)
	case []byte:
		buff = source.([]byte)
	default:
		buff, err = json.Marshal(source)
		if err != nil {
			return "", err
		}
	}
	text := utils.BytesToString(buff)
	return text, nil
}

func deserialize(text string, source interface{}) (interface{}, error) {
	if source == nil {
		return text, nil
	}
	var buff []byte
	var err error
	buff = util.StringToBytes(text)
	switch source.(type) {
	case int:
		return int(binary.LittleEndian.Uint32(buff)), nil
	case float32:
		return utils.ByteToFloat32(buff), nil
	case []byte:
		return buff, nil
	default:
		err = json.Unmarshal(buff, source)
	}
	return source, err
}
