package easymr

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/artifacts/iexecutor"

	"github.com/xp/shorttext-db/easymr/artifacts/task"
	"github.com/xp/shorttext-db/easymr/cmd"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/easymr/constants"

	"github.com/xp/shorttext-db/easymr/interfaces"
	"github.com/xp/shorttext-db/easymr/store"
	"github.com/xp/shorttext-db/easymr/utils"
	"github.com/xp/shorttext-db/glogger"
	"golang.org/x/time/rate"
	"net/http"
	"time"
)

var logger = glogger.MustGetLogger("collaborate")

func Set(key string, val ...interface{}) interface{} {
	cmd.Init()
	switch key {
	case constants.MAPPER:
		fs := store.GetInstance()
		fs.SetMapper(val[0].(interfaces.IMapper), val[1].(string))
	case constants.REDUCER:
		fs := store.GetInstance()
		fs.SetReducer(val[0].(interfaces.IReducer), val[1].(string))
	case constants.EXECUTOR:
		fs := store.GetInstance()
		fs.SetExecutor(val[0].(iexecutor.IExecutor), val[1].(string))

	case constants.CONSUMER:
		fs := store.GetInstance()
		fs.SetConsumer(val[0].(interfaces.IConsumer), val[1].(string))
	case constants.JOB_HANDLER:
		fs := store.GetInstance()
		fs.SetJobHandler(val[0].(interfaces.IJobHandler), val[1].(string))
	case constants.FUNCTION:
		// register function
		fs := store.GetInstance()
		f := val[0].(func(source *task.Collection,
			result *task.Collection,
			context *task.TaskContext) bool)
		if len(val) > 1 {
			fs.Add(f, val[1].(string))
			break
		}
		fs.Add(f)
	case constants.HASH_FUNCTION:
		// register hash function
		fs := store.GetInstance()
		f := val[0].(func(source *task.Collection,
			result *task.Collection,
			context *task.TaskContext) bool)
		return fs.HAdd(f)
	case constants.SHARED:
		fs := store.GetInstance()

		methods := val[0].([]string)
		handlers := make([]func(w http.ResponseWriter, r *http.Request, bg *task.Background), len(val)-1)
		for i, v := range val[1:] {
			handlers[i] = v.(func(w http.ResponseWriter, r *http.Request, bg *task.Background))
		}

		// register jobs
		fs.AddShared(methods, handlers...)
	case constants.LOCAL:
		fs := store.GetInstance()

		methods := val[0].([]string)
		handlers := make([]func(w http.ResponseWriter, r *http.Request, bg *task.Background), len(val)-1)
		for i, v := range val[1:] {
			handlers[i] = v.(func(w http.ResponseWriter, r *http.Request, bg *task.Background))
		}

		// register jobs
		fs.AddLocal(methods, handlers...)
	case constants.LIMIT:
		var (
			fs    = store.GetInstance()
			limit = constants.DEFAULT_JOB_REQUEST_REFILL_INTERVAL
			burst = constants.DEFAULT_JOB_REQUEST_BURST
		)
		if len(val) > 1 {
			limit = val[1].(time.Duration)
		}
		if len(val) > 2 {
			burst = val[2].(int)
		}
		fs.SetLimiter(val[0].(string), rate.Every(limit), burst)
	case constants.PROJECT_PATH:
		constants.PROJECT_DIR = val[0].(string)
	case constants.PROJECT_UNIX_PATH:
		constants.PROJECT_UNIX_DIR = val[0].(string)
	}
	return nil
}

func Run() {

	//runVars := cmd.Vars()

	// set handler for router
	router := store.GetRouter()

	router = utils.AdaptRouterToDebugMode(router)
	// create collaborator
	//clbt := collaborator.NewCollaborator(config.GetConfig().WorkerPerMaster)
	//clbt := collaborator.GetCollaborator()
	collaborator.GetCollaborator()
	//mst := master.NewMaster()
	//mst.BatchAttach(runVars.WorkerPerMaster)
	//mst.LaunchAll()

	//启动了GRPC服务器
	//clbt.Join(clbt.Workable)

	//if cmd.Vars().Principal {
	//	go func() {
	//		grpcServer := mrgrpc.NewGRPCServer(":"+strconv.Itoa(int(clbt.CardCase.Coordinator.Port)), clbt, serverCount)
	//		grpcServer.Start()
	//	}()
	//}
	httpPort := config.GetCase().Local.HttpPort
	serv := &http.Server{
		Addr:        httpPort,
		Handler:     router,
		ReadTimeout: constants.DEFAULT_READ_TIMEOUT,
	}
	err := serv.ListenAndServe()
	if err != nil {
		logger.Error("Http服务启动错误:", err)
	}

	//logger.Info("服务器完成启动.............................")
}
