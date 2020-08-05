package constants

import (
	"errors"
	"os"
	"time"
)

// system vars
const (
	DEBUG_INACTIVATED         = false
	DEBUG_ACTIVATED           = true
	DEFAULT_LISTEN_PORT       = 8080
	DEFAULT_CASE_PATH         = "/opt/case.json"
	DEFAULT_LOG_PATH          = "/opt/glogger.log"
	DEFAULT_LOG_PREFIX        = ""
	CLEAN_HISTORY             = true
	DEFAULT_NOT_CLEAN_HISTORY = false
	MAPPER                    = "Mapper"
	REDUCER                   = "Reducer"
	EXECUTOR                  = "Executor"
	FUNCTION                  = "Function"
	CONSUMER                  = "Consumer"
	JOB_HANDLER               = "JobHandler"
	REDUCER_WITH_RESULT       = "ReducerWithResult"
	HASH_FUNCTION             = "HashFunction"
	SHARED                    = "Shared"
	LOCAL                     = "Local"
	LIMIT                     = "Limit"
	PROJECT_PATH              = "ProjectPath"
	PROJECT_UNIX_PATH         = "ProjectUnixPath"
	DEFAULT_WORKER_PER_MASTER = 10
	DEFAULT_MAX_TASK_COUNT    = 100
	DEFAULT_HOST              = "localhost"
	DEFAULT_GOSSIP_NUM        = 10
	DEFAULT_CASE_ID           = "MyMRCalculator"
	DEFAULT_JOB_REQUEST_BURST = 1000
	JOB_RESULT                = "JobResult"
	TYPE_REGISTER             = "TypeRegister"
	MAX_TASK_COUNT            = "MAX_TASK_COUNT"
	DEFAULT_PRINCIPAL         = false

	CONTEXT_INFOMESSAGE   = "INFO"
	RESULT_FINISHED_PARAM = "1"
	EMPTY_TASK_TYPE       = 1000
)

// store setting
const (
	DEFAULT_HASH_LENGTH = 12
)

// time consts
var (
	DEFAULT_RPC_DIAL_TIMEOUT       = 10 * time.Second
	DEFAULT_READ_TIMEOUT           = 600 * time.Second
	DEFAULT_PERIOD_SHORT           = 60 * time.Second
	DEFAULT_PERIOD_LONG            = 600 * time.Second
	DEFAULT_PERIOD_ROUTINE_DAY     = 24 * time.Hour
	DEFAULT_PERIOD_ROUTINE_WEEK    = 7 * 24 * time.Hour
	DEFAULT_PERIOD_ROUTINE_30_DAYS = 30 * DEFAULT_PERIOD_ROUTINE_DAY
	DEFAULT_PERIOD_PERMANENT       = 1 * time.Second
	DEFAULT_TASK_EXPIRY_TIME       = 1800 * time.Second

	DEFAULT_RESPONSE_EXPIRY_TIME = 120 * time.Second

	DEFAULT_GC_INTERVAL                  = 180 * time.Second
	DEFAULT_MAX_MAPPING_TIME             = 600 * time.Second
	DEFAULT_SYNC_INTERVAL                = 10 * time.Second
	DEFAULT_HEARTBEAT_INTERVAL           = 30 * time.Second
	DEFAULT_JOB_REQUEST_REFILL_INTERVAL  = 10 * time.Millisecond
	DEFAULT_STAT_FLUSH_INTERVAL          = 20 * time.Millisecond
	DEFAULT_STAT_ABSTRACT_INTERVAL       = 3 * time.Second
	DEFAULT_COLLABORATOR_EXPIRY_INTERVAL = 10 * time.Minute
)

// executor types
const (
	EXECUTOR_TYPE_MAPPER   = "mapper"
	EXECUTOR_TYPE_REDUCER  = "reducer"
	EXECUTOR_TYPE_COMBINER = "combiner"
	EXECUTOR_TYPE_DEFAULT  = "default"
	TASK_TERM              = "term"
	TASK_INDEX             = "index"

	//请求处理超时
	NODE_REQUEST_TIMEOUT_ERROR = 1000
	NODE_REQUEST_TIMEOUT       = 3600
)

// communication types
const (
	ARG_TYPE_INTEGER               = "integer"
	ARG_TYPE_NUMBER                = "number"
	ARG_TYPE_STRING                = "string"
	ARG_TYPE_OBJECT                = "object"
	ARG_TYPE_BOOLEAN               = "boolean"
	ARG_TYPE_NULL                  = "null"
	ARG_TYPE_ARRAY                 = "array"
	CONSTRAINT_TYPE_MAX            = "maximum"
	CONSTRAINT_TYPE_MIN            = "minimum"
	CONSTRAINT_TYPE_XMAX           = "exclusiveMaximum"
	CONSTRAINT_TYPE_XMIN           = "exclusiveMinimum"
	CONSTRAINT_TYPE_UNIQUE_ITEMS   = "uniqueItems"
	CONSTRAINT_TYPE_MAX_PROPERTIES = "maxProperties"
	CONSTRAINT_TYPE_MIN_PROPERTIES = "minProperties"
	CONSTRAINT_TYPE_MAX_LENGTH     = "maxLength"
	CONSTRAINT_TYPE_MIN_LENGTH     = "minLength"
	CONSTRAINT_TYPE_PATTERN        = "pattern"
	CONSTRAINT_TYPE_MAX_ITEMS      = "maxItems"
	CONSTRAINT_TYPE_MIN_ITEMS      = "minItems"
	CONSTRAINT_TYPE_ENUM           = "enum" // value of interface{} should accept a slice
	CONSTRAINT_TYPE_ALLOF          = "allOf"
	CONSTRAINT_TYPE_ANYOF          = "anyOf"
	CONSTRAINT_TYPE_ONEOF          = "oneOf"
)

const (
	STATS_POLICY_SUM_OF_INTS = "StatsPolicySumOfInt"
)

// errors
var (
	ERR_UNKNOWN_CMD_ARG                    = errors.New("goplatform: unknown commandline argument, please enter -h to check out")
	ERR_CONNECTION_CLOSED                  = errors.New("goplatform: connection closed")
	ERR_UNKNOWN                            = errors.New("goplatform: unknown error")
	ERR_API                                = errors.New("goplatform: api error")
	ERR_NO_COLLABORATOR                    = errors.New("goplatform: peer does not exist")
	ERR_COLLAOBRATOR_ALREADY_EXISTS        = errors.New("goplatform: collaborator already exists")
	ERR_TASK_TIMEOUT                       = errors.New("goplatform: task timeout error")
	ERR_NO_PEERS                           = errors.New("goplatform: no peer appears in the contact")
	ERR_FUNCT_NOT_EXIST                    = errors.New("goplatform: no such function found in store")
	ERR_JOB_NOT_EXIST                      = errors.New("goplatform: no sucn job found in store")
	ERR_EXECUTOR_NOT_FOUND                 = errors.New("goplatform: no such executor found in store")
	ERR_LIMITER_NOT_FOUND                  = errors.New("goplatform: no such limiter found in store")
	ERR_VAL_NOT_FOUND                      = errors.New("goplatform: no value found with such key")
	ERR_CASE_MISMATCH                      = errors.New("goplatform: case mismatch error")
	ERR_COLLABORATOR_MISMATCH              = errors.New("goplatform: collaborator mismatch error")
	ERR_UNKNOWN_MSG_TYPE                   = errors.New("goplatform: unknown message type error")
	ERR_TASK_MAP_FAIL                      = errors.New("goplatform: map operation failing error")
	ERR_TASK_REDUCE_FAIL                   = errors.New("goplatform: reduce operation failing error")
	ERR_EXECUTOR_STACK_LENGTH_INCONSISTENT = errors.New("goplatform: executor stack length inconsistent error")
	ERR_MSG_CHAN_DIRTY                     = errors.New("goplatform: message channel has unconsumed message error")
	ERR_TASK_CHAN_DIRTY                    = errors.New("goplatform: task channel has unconsumed task error")
	ERR_STAT_TYPE_NOT_FOUND                = errors.New("goplatform: stat type not found error")
	ERR_INPUT_STREAM_CORRUPTED             = errors.New("goplatform: input stream corrupted error")
	ERR_INPUT_STREAM_NOT_SUPPORTED         = errors.New("goplatform: input stream type not suppoted error")
	ERR_IO_DECODE_POINTER_REQUIRED         = errors.New("goplatform: Decode error, the reference instance must be a pointer")
	ERR_IO_DECODE_SLICE_REQUIRED           = errors.New("goplatform: Decode error, the reference instance must be a slice")
	ERR_IO_DECODE_STRUCT_REQUIRED          = errors.New("goplatform: Decode error, the reference instance must be a struct")
)

type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// HTTP headers
var (
	HEADER_CONTENT_TYPE_JSON       = Header{"Content-Type", "application/json"}
	HEADER_CONTENT_TYPE_TEXT       = Header{"Content-Type", "text/html"}
	HEADER_CORS_ENABLED_ALL_ORIGIN = Header{"Access-Control-Allow-Origin", "*"}
)

// Gossip Protocol headers
var (
	GOSSIP_HEADER_OK                    = Header{"200", "OK"}
	GOSSIP_HEADER_UNKNOWN_MSG_TYPE      = Header{"400", "UnknownMessageType"}
	GOSSIP_HEADER_CASE_MISMATCH         = Header{"401", "CaseMismatch"}
	GOSSIP_HEADER_COLLABORATOR_MISMATCH = Header{"401", "CollaboratorMismatch"}
	GOSSIP_HEADER_UNKNOWN_ERROR         = Header{"500", "UnknownGossipError"}
)

// restful
const (
	JSON_API_VERSION = `{"version":"1.0"}`
)

var (
	PROJECT_DIR      = ""
	PROJECT_UNIX_DIR = ""
	LIB_DIR          = "github.com/xp/shorttext-db/easymr/"
	LIB_UNIX_DIR     = os.Getenv("GOPATH") + "/src/github.com/xp/shorttext-db/easymr/"
)

const (
	LARGE_BLOCK_TASK_TYPE = 1
)

const (
	WORKER_ID = "WorkerId"
)
