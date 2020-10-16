package config

const (
	MSG_KV_GET   = 1001
	MSG_KV_SET   = 1002
	MSG_KV_DEL   = 1003
	MSG_KV_ClOSE = 1004

	MSG_KV_TEXTGET = 1005
	MSG_KV_TEXTSET = 1006
	MSG_KV_FIND    = 1007

	MSG_KV_GET_RESULT = 2001
	MSG_KV_SET_RESULT = 2002
	MSG_KV_DEL_RESULT = 2003

	MSG_KV_RESULT_SUCCESS = 3001
	MSG_KV_RESULT_FAILURE = 3002
)

const (
	PARSING_RECORD_SEP = "\\"
)
const (
	GJSON_FIELD_ID   = "id"
	GJSON_FIELD_DESC = "desc"
)
