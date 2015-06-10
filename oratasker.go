// taskerOra
package otasker

import (
	"bytes"
	"fmt"
	"gopkg.in/errgo.v1"
	"gopkg.in/goracle.v1/oracle"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	StatusErrorPage                 = 561
	StatusWaitPage                  = 562
	StatusBreakPage                 = 563
	StatusRequestWasInterrupted     = 564
	StatusInvalidUsernameOrPassword = 565
	StatusInsufficientPrivileges    = 566
	StatusAccountIsLocked           = 567
)

type OracleTaskResult struct {
	StatusCode  int
	ContentType string
	Headers     string
	Content     []byte
	Duration    int64
}
type OracleTasker interface {
	Run(sessionID,
		taskID,
		userName,
		userPass,
		connStr,
		paramStoreProc,
		beforeScript,
		afterScript,
		documentTable string,
		cgiEnv map[string]string,
		procName string,
		urlParams url.Values,
		reqFiles *Form,
		dumpErrorFileName string) OracleTaskResult
	CloseAndFree() error
	Break() error
	Info() OracleTaskInfo
}

type oracleTaskerStep struct {
	stepBg        time.Time
	stepFn        time.Time
	stepStm       string
	stepStmParams map[string]interface{}
	stepSuccess   bool
}

type oracleTaskerMessageLog struct {
	//task  sessionTask
	sessionID string
	taskID    string
	userName  string
	userPass  string
	connStr   string
	procName  string
	steps     map[string]*oracleTaskerStep
}

type oracleTasker struct {
	sync.Mutex                   //включается только при изменении данных, используемых в Break() и Info()
	cafMutex          sync.Mutex // требуется для синхронизации разрушения объекта C(lose )A(nd )F(ree )Mutes
	sendOp            func(op *OracleOperation)
	streamID          string
	conn              *oracle.Connection
	descr             OracleDescriber
	connUserName      string
	connUserPass      string
	connStr           string
	sessID            string
	logs              []oracleTaskerMessageLog
	logCurr           *oracleTaskerMessageLog
	stateIsWorking    bool
	stateCreateDT     time.Time
	stateLastFinishDT time.Time
	stmEvalSessionID  string
	stmMain           string
	stmGetRestChunk   string
	stmKillSession    string
	stmFileUpload     string
}

func newOracleProcTasker(f func(op *OracleOperation), stmEvalSessionID, stmMain, stmGetRestChunk, stmKillSession, stmFileUpload, streamID string) OracleTasker {
	r := oracleTasker{}
	r.sendOp = f
	r.streamID = streamID
	r.stateIsWorking = false
	r.stateCreateDT = time.Now()
	r.stateLastFinishDT = time.Time{}
	r.descr = NewOracleDescriber()
	r.logs = make([]oracleTaskerMessageLog, 0)
	r.stmEvalSessionID = stmEvalSessionID
	r.stmMain = stmMain
	r.stmGetRestChunk = stmGetRestChunk
	r.stmKillSession = stmKillSession
	r.stmFileUpload = stmFileUpload
	return &r
}

func (r *oracleTasker) CloseAndFree() error {
	r.cafMutex.Lock()
	defer r.cafMutex.Unlock()

	r.descr.Clear()
	r.descr = nil
	if r.conn != nil {
		bg := time.Now()
		if err := r.conn.Close(); err != nil {
			logoutOp(r.sendOp, r.streamID, r.connUserName, r.connUserPass, r.connStr, bg, time.Now(), false)
			return err
		}
		logoutOp(r.sendOp, r.streamID, r.connUserName, r.connUserPass, r.connStr, bg, time.Now(), true)
	}
	r.logCurr = nil
	r.logs = nil
	return nil
}

func (r *oracleTasker) Run(sessionID, taskID, userName, userPass, connStr,
	paramStoreProc, beforeScript, afterScript, documentTable string,
	cgiEnv map[string]string, procName string, urlParams url.Values,
	reqFiles *Form, dumpErrorFileName string) OracleTaskResult {
	var bgMem runtime.MemStats
	runtime.ReadMemStats(&bgMem)

	r.cafMutex.Lock()

	func() {
		r.Lock()
		defer r.Unlock()
		r.stateIsWorking = true
	}()

	func() {
		r.Lock()
		defer r.Unlock()
		r.logCurr = &oracleTaskerMessageLog{
			sessionID: sessionID,
			taskID:    taskID,
			userName:  userName,
			userPass:  userPass,
			connStr:   connStr,
			procName:  procName,
			steps:     make(map[string]*oracleTaskerStep),
		}
		r.logs = append(r.logs, *r.logCurr)
	}()

	defer func(bgMem *runtime.MemStats) {

		func() {
			r.Lock()
			defer r.Unlock()
			r.logCurr = nil
			r.stateIsWorking = false
			r.stateLastFinishDT = time.Now()
		}()

		r.cafMutex.Unlock()

		var fnMem runtime.MemStats
		runtime.ReadMemStats(&fnMem)
		//		log.Println("bg Alloc       => ", bgMem.Alloc)
		//		log.Println("bg TotalAlloc  => ", bgMem.TotalAlloc)
		//		log.Println("bg HeapAlloc   => ", bgMem.HeapAlloc)
		//		log.Println("bg HeapSys     => ", bgMem.HeapSys)
		//		log.Println("************************************")
		//		log.Println("fn Alloc       => ", fnMem.Alloc)
		//		log.Println("fn TotalAlloc  => ", fnMem.TotalAlloc)
		//		log.Println("fn HeapAlloc   => ", fnMem.HeapAlloc)
		//		log.Println("fn HeapSys     => ", fnMem.HeapSys)
		//		log.Println("************************************")
		//		log.Println("dev Alloc      => ", fnMem.Alloc-bgMem.Alloc)
		//		log.Println("dev TotalAlloc => ", fnMem.TotalAlloc-bgMem.TotalAlloc)
		//		log.Println("dev HeapAlloc  => ", fnMem.HeapAlloc-bgMem.HeapAlloc)
		//		log.Println("dev HeapSys    => ", fnMem.HeapSys-bgMem.HeapSys)
		//		log.Println("************************************")
	}(&bgMem)

	bg := time.Now()
	var res = OracleTaskResult{}
	if err := r.connect(userName, userPass, connStr); err != nil {
		res.StatusCode, res.Content = packError(err)
		// Формируем дамп до закрытия соединения, чтобы получить корректный запрос из последнего шага
		r.dumpError(userName, connStr, dumpErrorFileName, err)
		if res.StatusCode == StatusRequestWasInterrupted {
			//Если произошла ошибка, всегда закрываем соединение с БД
			r.disconnect()
		}

		res.Duration = int64(time.Since(bg) / time.Second)
		return res
	}

	if err := r.run(&res, paramStoreProc, beforeScript, afterScript, documentTable,
		cgiEnv, procName, urlParams, reqFiles); err != nil {
		res.StatusCode, res.Content = packError(err)
		// Формируем дамп до закрытия соединения, чтобы получить корректный запрос из последнего шага
		r.dumpError(userName, connStr, dumpErrorFileName, err)
		if res.StatusCode == StatusRequestWasInterrupted {
			//Если произошла ошибка, всегда закрываем соединение с БД
			r.disconnect()
		}
		res.Duration = int64(time.Since(bg) / time.Second)
		return res
	}
	res.StatusCode = http.StatusOK
	res.Duration = int64(time.Since(bg) / time.Second)
	return res
}

func (r *oracleTasker) connect(username, userpass, connstr string) (err error) {

	if (r.conn == nil) || (r.connUserName != username) || (r.connUserPass != userpass) || (r.connStr != connstr) {
		r.disconnect()

		return func() error {
			stepName := r.openStep("connect")
			defer r.closeStep(stepName)
			r.setStepInfo(stepName, "connect", nil, false)
			bg := time.Now()
			r.conn, err = oracle.NewConnection(username, userpass, connstr, false)
			if err != nil {
				loginOp(r.sendOp, r.streamID, username, userpass, connstr, bg, time.Now(), false)
				// Если выходим с ошибкой, то в вызывающей процедуре будет вызван disconnect()
				return err
			}
			loginOp(r.sendOp, r.streamID, username, userpass, connstr, bg, time.Now(), true)
			r.connUserName = username
			r.connUserPass = userpass
			r.connStr = connstr
			// Соединение с БД прошло успешно.
			if err = r.evalSessionID(); err != nil {
				// Если выходим с ошибкой, то в вызывающей процедуре будет вызван disconnect()
				return err
			}
			r.setStepInfo(stepName, "connect", nil, true)
			return nil
		}()
	}
	if !r.conn.IsConnected() {
		panic("Сюда приходить никогда не должны !!!")
	}

	return nil
}

func (r *oracleTasker) disconnect() (err error) {
	if r.conn != nil {
		if r.conn.IsConnected() {
			stepName := r.openStep("disconnect")
			r.setStepInfo(stepName, "disconnect", nil, false)

			r.Lock()

			defer func() {
				r.conn = nil
				r.connUserName = ""
				r.connUserPass = ""
				r.connStr = ""
				r.sessID = ""
				r.Unlock()
				r.setStepInfo(stepName, "disconnect", nil, true)
				r.closeStep(stepName)

			}()

			if r.conn != nil {
				bg := time.Now()
				err = r.conn.Close()
				if err != nil {
					logoutOp(r.sendOp, r.streamID, r.connUserName, r.connUserPass, r.connStr, bg, time.Now(), false)
					return err
				}
				logoutOp(r.sendOp, r.streamID, r.connUserName, r.connUserPass, r.connStr, bg, time.Now(), true)
			}
		}
	}
	return nil
}

func (r *oracleTasker) execStm(cur *oracle.Cursor, streamID, stm string, params map[string]interface{}) error {
	bg := time.Now()
	var (
		err            error
		serverBgVar    *oracle.Variable
		serverFnVar    *oracle.Variable
		serverFnScnVar *oracle.Variable
	)

	serverBgVar, err = cur.NewVariable(0, oracle.StringVarType, 100)
	if err != nil {
		return errgo.Newf("error creating variable for %s: %s", "serverBg", err)
	}
	serverFnVar, err = cur.NewVariable(0, oracle.StringVarType, 100)
	if err != nil {
		return errgo.Newf("error creating variable for %s: %s", "serverFn", err)
	}
	serverFnScnVar, err = cur.NewVariable(0, oracle.StringVarType, 200)
	if err != nil {
		return errgo.Newf("error creating variable for %s: %s", "serverFnScn", err)
	}

	params["server_bg"] = serverBgVar
	params["server_fn"] = serverFnVar
	params["server_fn_scn"] = serverFnScnVar

	if err := cur.Execute(stm, nil, params); err != nil {
		execOp(r.sendOp, streamID, r.connUserName, r.connUserPass, r.connStr, stm, params, bg, time.Now(), false)
		return err
	}
	execOp(r.sendOp, streamID, r.connUserName, r.connUserPass, r.connStr, stm, params, bg, time.Now(), true)
	return nil
}

func (r *oracleTasker) evalSessionID() error {
	stepName := r.openStep("evalSessionID")

	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepName) }()

	var (
		err error
		v   *oracle.Variable
	)
	sessID := ""
	r.sessID = sessID
	v, err = cur.NewVar(&sessID)
	if err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", sessID, sessID, err)
	}

	stepStm := r.stmEvalSessionID
	stepStmParams := map[string]interface{}{"sid": v}
	r.setStepInfo(stepName, stepStm, stepStmParams, false)

	err = r.execStm(cur, r.streamID, stepStm, stepStmParams)

	r.sessID = sessID
	r.setStepInfo(stepName, stepStm, stepStmParams, err == nil)
	return err
}

func (r *oracleTasker) run(res *OracleTaskResult, paramStoreProc, beforeScript, afterScript, documentTable string,
	cgiEnv map[string]string, procName string, urlParams url.Values, reqFiles *Form) error {
	const (
		stmParamsInit = `
  l_num_params := :num_params;
  l_param_name := :param_name;
  l_param_val := :param_val;
  l_num_ext_params := :num_ext_params;
  l_ext_param_name := :ext_param_name;
  l_ext_param_val := :ext_param_val;
  l_package_name := :package_name;
`
	)
	var (
		err                 error
		numParamsVar        *oracle.Variable
		paramNameVar        *oracle.Variable
		paramValVar         *oracle.Variable
		ContentTypeVar      *oracle.Variable
		ContentLengthVar    *oracle.Variable
		CustomHeadersVar    *oracle.Variable
		rcVar               *oracle.Variable
		contentVar          *oracle.Variable
		bNextChunkExistsVar *oracle.Variable
		lobVar              *oracle.Variable
		sqlErrCodeVar       *oracle.Variable
		sqlErrMVar          *oracle.Variable
		sqlErrTraceVar      *oracle.Variable
	)
	stepName := r.openStep("run")
	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepName) }()

	numParams := int64(len(cgiEnv))
	var (
		paramName       []interface{}
		paramVal        []interface{}
		paramNameMaxLen int
		paramValMaxLen  int
	)

	for key, val := range cgiEnv {
		paramName = append(paramName, key)
		paramVal = append(paramVal, val)

		if len(key) > paramNameMaxLen {
			paramNameMaxLen = len(key)
		}
		if len(val) > paramValMaxLen {
			paramValMaxLen = len(val)
		}
	}

	if numParamsVar, err = cur.NewVar(&numParams); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", numParams, numParams, err)
	}
	if paramNameVar, err = cur.NewArrayVar(oracle.StringVarType, paramName, uint(paramNameMaxLen)); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", paramName, paramName, err)
	}
	if paramValVar, err = cur.NewArrayVar(oracle.StringVarType, paramVal, uint(paramValMaxLen)); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", paramVal, paramVal, err)
	}

	if ContentTypeVar, err = cur.NewVariable(0, oracle.StringVarType, 1024); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "ContentType", "varchar2(32767)", err)
	}
	if ContentLengthVar, err = cur.NewVariable(0, oracle.Int32VarType, 0); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "ContentLength", "number", err)
	}
	if CustomHeadersVar, err = cur.NewVariable(0, oracle.StringVarType, 32767); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "CustomHeaders", "varchar2(32767)", err)
	}
	if rcVar, err = cur.NewVariable(0, oracle.Int32VarType, 0); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "rc__", "number", err)
	}
	if contentVar, err = cur.NewVariable(0, oracle.StringVarType, 32767); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "content__", "varchar2(32767)", err)
	}
	if bNextChunkExistsVar, err = cur.NewVariable(0, oracle.Int32VarType, 0); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "bNextChunkExists", "number", err)
	}
	if lobVar, err = cur.NewVariable(0, oracle.BlobVarType, 0); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "BlobVarType", "BlobVarType", err)
	}

	if sqlErrCodeVar, err = cur.NewVariable(0, oracle.Int32VarType, 0); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "sqlErrCode", "number", err)
	}

	if sqlErrMVar, err = cur.NewVariable(0, oracle.StringVarType, 32767); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "sqlErrM", "varchar2(32767)", err)
	}

	if sqlErrTraceVar, err = cur.NewVariable(0, oracle.StringVarType, 32767); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "sqlErrTrace", "varchar2(32767)", err)
	}

	sqlParams := map[string]interface{}{"num_params": numParamsVar,
		"param_name":       paramNameVar,
		"param_val":        paramValVar,
		"ContentType":      ContentTypeVar,
		"ContentLength":    ContentLengthVar,
		"CustomHeaders":    CustomHeadersVar,
		"rc__":             rcVar,
		"content__":        contentVar,
		"lob__":            lobVar,
		"bNextChunkExists": bNextChunkExistsVar,
		"sqlerrcode":       sqlErrCodeVar,
		"sqlerrm":          sqlErrMVar,
		"sqlerrtrace":      sqlErrTraceVar}

	sParams := ""
	sParamStore := ""

	stepStm := "Describe " + procName
	dp, err := r.descr.Describe(r, r.conn, procName)
	if err != nil {
		return err
	}

	var (
		extParamName        []interface{}
		extParamValue       []interface{}
		extParamNameMaxLen  int
		extParamValueMaxLen int
	)
	for paramName, paramValue := range urlParams {
		//fmt.Println(paramName, " ", paramValue, " ", dp.ParamDataType(paramName), " ", dp.ParamDataSubType(paramName))
		err := prepareParam(cur, paramName, paramValue,
			dp.ParamDataType(paramName), dp.ParamDataSubType(paramName),
			paramStoreProc, sqlParams, &sParams, &sParamStore)
		if err != nil {
			return err
		}

		extParamName = append(extParamName, paramName)
		extParamValue = append(extParamValue, paramValue[0])

		if len(paramName) > extParamNameMaxLen {
			extParamNameMaxLen = len(paramName)
		}

		if len(paramValue[0]) > extParamValueMaxLen {
			extParamValueMaxLen = len(paramValue[0])
		}
		//		if len(paramValue) > 1 {
		//			panic("Len too long")
		//		}
	}

	if reqFiles != nil {
		for paramName, paramValue := range reqFiles.File {
			fileName, err := r.saveFile(paramStoreProc, beforeScript, afterScript, documentTable,
				cgiEnv, urlParams, paramValue)
			if err != nil {
				return err
			}
			err = prepareParam(cur, paramName, fileName,
				dp.ParamDataType(paramName), dp.ParamDataSubType(paramName),
				paramStoreProc, sqlParams, &sParams, &sParamStore)
			if err != nil {
				return err
			}

			extParamName = append(extParamName, paramName)
			extParamValue = append(extParamValue, fileName[0])

			if len(fileName[0]) > extParamValueMaxLen {
				extParamValueMaxLen = len(fileName[0])
			}

		}
	}

	if sqlParams["ext_param_name"], err = cur.NewArrayVar(oracle.StringVarType, extParamName, uint(extParamNameMaxLen)); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "ext_param_name", "varchar2", err)
	}

	if sqlParams["ext_param_val"], err = cur.NewArrayVar(oracle.StringVarType, extParamValue, uint(extParamValueMaxLen)); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "ext_param_val", "varchar2", err)
	}

	epn := int32(len(extParamName))
	if sqlParams["num_ext_params"], err = cur.NewVar(&epn); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "num_ext_params", "number", err)
	}

	pkgName := dp.PackageName()
	if sqlParams["package_name"], err = cur.NewVar(&pkgName); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "package_name", "varchar2", err)
	}

	stepStm = fmt.Sprintf(r.stmMain, "", stmParamsInit, beforeScript, sParamStore, procName, sParams, afterScript)
	stepStmParams := sqlParams

	r.setStepInfo(stepName, stepStm, stepStmParams, false)
	if err := r.execStm(cur, r.streamID, stepStm, stepStmParams); err != nil {
		return err
	}

	sqlErrCode, err := sqlErrCodeVar.GetValue(0)
	if err != nil {
		return err
	}

	if sqlErrCode.(int32) != 0 {
		sqlErrM, err := sqlErrMVar.GetValue(0)
		if err != nil {
			return err
		}
		sqlErrTrace, err := sqlErrTraceVar.GetValue(0)
		if err != nil {
			return err
		}
		return oracle.NewErrorAt(int(sqlErrCode.(int32)), sqlErrM.(string), sqlErrTrace.(string))
	}

	ct, err := ContentTypeVar.GetValue(0)
	if err != nil {
		return err
	}
	// Поскольку буфер ВСЕГДА формируем в UTF-8,
	// нужно изменить значение Charset в ContentType
	contentType := "text/html"
	if ct != nil {
		contentType = ct.(string)
	}
	res.ContentType = contentType

	ch, err := CustomHeadersVar.GetValue(0)
	if err != nil {
		return err
	}
	if ch != nil {
		res.Headers = ch.(string)
	}

	rc, err := rcVar.GetValue(0)
	if err != nil {
		return err
	}
	switch rc.(int32) {
	case 0:
		{
			// Oracle возвращает данные ВСЕГДА в UTF-8
			data, err := contentVar.GetValue(0)
			if err != nil {
				return err
			}
			if data == nil {
				return nil
			}
			// Oracle возвращает данные ВСЕГДА в UTF-8
			res.Content = append(res.Content, []byte(data.(string))...)
			bNextChunkExists, err := bNextChunkExistsVar.GetValue(0)
			if err != nil {
				return err
			}
			if bNextChunkExists.(int32) != 0 {
				r.getRestChunks(res)
			}
		}
	case 1:
		{
			data, err := lobVar.GetValue(0)
			if err != nil {
				return err
			}
			ext, ok := data.(*oracle.ExternalLobVar)
			if !ok {
				return errgo.Newf("data is not *ExternalLobVar, but %T", data)
			}
			if ext != nil {
				size, err := ext.Size(false)
				if err != nil {
					fmt.Println("size error: ", err)
				}
				if size != 0 {
					buf, err := ext.ReadAll()
					if err != nil {
						return err
					}
					res.Content = append(res.Content, buf...)
				}
			}

		}
	}

	r.setStepInfo(stepName, stepStm, stepStmParams, true)
	return nil
}

func (r *oracleTasker) getRestChunks(res *OracleTaskResult) error {
	var (
		err                 error
		bNextChunkExists    int64
		DataVar             *oracle.Variable
		bNextChunkExistsVar *oracle.Variable
		sqlErrCodeVar       *oracle.Variable
		sqlErrMVar          *oracle.Variable
		sqlErrTraceVar      *oracle.Variable
	)
	stepName := r.openStep("getRestChunks")
	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepName) }()

	if DataVar, err = cur.NewVariable(0, oracle.StringVarType, 32767); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "Data", "string", err)
	}

	if bNextChunkExistsVar, err = cur.NewVar(&bNextChunkExists); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", bNextChunkExists, bNextChunkExists, err)
	}
	if sqlErrCodeVar, err = cur.NewVariable(0, oracle.Int32VarType, 0); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "sqlErrCode", "number", err)
	}

	if sqlErrMVar, err = cur.NewVariable(0, oracle.StringVarType, 32767); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "sqlErrM", "varchar2(32767)", err)
	}

	if sqlErrTraceVar, err = cur.NewVariable(0, oracle.StringVarType, 32767); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "sqlErrTrace", "varchar2(32767)", err)
	}

	stepStm := r.stmGetRestChunk
	stepStmParams := map[string]interface{}{
		"Data":             DataVar,
		"bNextChunkExists": bNextChunkExistsVar,
		"sqlerrcode":       sqlErrCodeVar,
		"sqlerrm":          sqlErrMVar,
		"sqlerrtrace":      sqlErrTraceVar,
	}
	r.setStepInfo(stepName, stepStm, stepStmParams, false)

	bNextChunkExists = 1

	for bNextChunkExists != 0 {
		if err := r.execStm(cur, r.streamID, stepStm, stepStmParams); err != nil {
			return err
		}
		sqlErrCode, err := sqlErrCodeVar.GetValue(0)
		if err != nil {
			return err
		}

		if sqlErrCode.(int32) != 0 {
			sqlErrM, err := sqlErrMVar.GetValue(0)
			if err != nil {
				return err
			}
			sqlErrTrace, err := sqlErrTraceVar.GetValue(0)
			if err != nil {
				return err
			}
			return oracle.NewErrorAt(int(sqlErrCode.(int32)), sqlErrM.(string), sqlErrTrace.(string))
		}
		data, err := DataVar.GetValue(0)
		if err != nil {
			return err
		}
		// Oracle возвращает данные ВСЕГДА в UTF-8
		res.Content = append(res.Content, []byte(data.(string))...)
	}
	r.setStepInfo(stepName, stepStm, stepStmParams, true)
	return nil
}

func (r *oracleTasker) saveFile(paramStoreProc, beforeScript, afterScript, documentTable string,
	cgiEnv map[string]string, urlParams url.Values, fileHeaders []*FileHeader) ([]string, error) {
	fileNames := make([]string, len(fileHeaders))
	for i, fileHeader := range fileHeaders {
		//_, fileNames[i] = filepath.Split(fileHeader.Filename)
		fileNames[i] = ExtractFileName(fileHeader.Header.Get("Content-Disposition"))

		fileReader, err := fileHeader.Open()
		if err != nil {
			return nil, err
		}
		fileContent, err := ioutil.ReadAll(fileReader)
		if err != nil {
			return nil, err
		}
		//_ = fileContent
		fileContentType := fileHeader.Header.Get("Content-Type")
		fileNames[i], err = r.saveFileToDB(paramStoreProc, beforeScript, afterScript, documentTable,
			cgiEnv, urlParams, fileNames[i], fileHeader.lastArg, fileContentType, fileContentType, fileContent)
		if err != nil {
			return nil, err
		}
	}
	return fileNames, nil
}

func (r *oracleTasker) saveFileToDB(paramStoreProc, beforeScript, afterScript, documentTable string,
	cgiEnv map[string]string, urlParams url.Values, fName, fItem, fMime, fContentType string, fContent []byte) (string, error) {

	stepName := r.openStep("saveFileToDB")
	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepName) }()

	numParams := int64(len(cgiEnv))
	var (
		paramName       []interface{}
		paramVal        []interface{}
		paramNameMaxLen int
		paramValMaxLen  int
	)

	for key, val := range cgiEnv {
		paramName = append(paramName, key)
		paramVal = append(paramVal, val)
		if len(key) > paramNameMaxLen {
			paramNameMaxLen = len(key)
		}
		if len(val) > paramValMaxLen {
			paramValMaxLen = len(val)
		}
	}

	numParamsVar, err := cur.NewVar(&numParams)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", numParams, numParams, err)
	}

	paramNameVar, err := cur.NewArrayVar(oracle.StringVarType, paramName, uint(paramNameMaxLen))
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", paramName, paramName, err)
	}

	paramValVar, err := cur.NewArrayVar(oracle.StringVarType, paramVal, uint(paramValMaxLen))
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", paramVal, paramVal, err)
	}

	nameVar, err := cur.NewVar(&fName)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", fName, fName, err)
	}

	mimeVar, err := cur.NewVar(&fMime)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", fMime, fMime, err)
	}

	ContentTypeVar, err := cur.NewVar(&fContentType)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", fContentType, fContentType, err)
	}

	docSize := len(fContent)
	docSizeVar, err := cur.NewVar(&docSize)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", docSize, docSize, err)
	}

	lobVar, err := cur.NewVariable(0, oracle.BlobVarType, uint(docSize))
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(lob): %s", "lob", err)
	}

	if err := lobVar.SetValue(0, fContent); err != nil {
		return "", errgo.Newf("error setting variable for %s(lob): %s", "lob", err)
	}

	itemID := fItem

	applicationID := urlParams.Get("p_flow_id")
	pageID := urlParams.Get("p_flow_step_id")
	sessionID := urlParams.Get("p_instance")
	request := urlParams.Get("p_request")

	itemIDVar, err := cur.NewVar(&itemID)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", itemID, itemID, err)
	}
	applicationIDVar, err := cur.NewVar(&applicationID)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", applicationID, applicationID, err)
	}

	pageIDVar, err := cur.NewVar(&pageID)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", pageID, pageID, err)
	}
	sessionIDVar, err := cur.NewVar(&sessionID)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", sessionID, sessionID, err)
	}
	requestVar, err := cur.NewVar(&request)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", request, request, err)
	}

	retName := ""
	retNameVar, err := cur.NewVar(&retName)
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", retName, retName, err)
	}

	var (
		sqlErrCodeVar  *oracle.Variable
		sqlErrMVar     *oracle.Variable
		sqlErrTraceVar *oracle.Variable
	)
	if sqlErrCodeVar, err = cur.NewVariable(0, oracle.Int32VarType, 0); err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", "sqlErrCode", "number", err)
	}

	if sqlErrMVar, err = cur.NewVariable(0, oracle.StringVarType, 32767); err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", "sqlErrM", "varchar2(32767)", err)
	}

	if sqlErrTraceVar, err = cur.NewVariable(0, oracle.StringVarType, 32767); err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", "sqlErrTrace", "varchar2(32767)", err)
	}
	//fmt.Println(fmt.Sprintf(r.stmFileUpload, task.beforeScript, task.documentTable))

	stepStm := fmt.Sprintf(r.stmFileUpload, beforeScript, documentTable)
	stepStmParams := map[string]interface{}{"num_params": numParamsVar,
		"param_name":     paramNameVar,
		"param_val":      paramValVar,
		"name":           nameVar,
		"mime_type":      mimeVar,
		"doc_size":       docSizeVar,
		"content_type":   ContentTypeVar,
		"lob":            lobVar,
		"item_id":        itemIDVar,
		"application_id": applicationIDVar,
		"page_id":        pageIDVar,
		"session_id":     sessionIDVar,
		"request":        requestVar,
		"ret_name":       retNameVar,
		"sqlerrcode":     sqlErrCodeVar,
		"sqlerrm":        sqlErrMVar,
		"sqlerrtrace":    sqlErrTraceVar}

	r.setStepInfo(stepName, stepStm, stepStmParams, false)

	//	if task.reqDumpStatements {
	//		r.dumpStm(task, step.stepStm, step.stepStmParams)
	//	}
	if err := r.execStm(cur, r.streamID, stepStm, stepStmParams); err != nil {
		return "", err
	}
	sqlErrCode, err := sqlErrCodeVar.GetValue(0)
	if err != nil {
		return "", err
	}

	if sqlErrCode.(int32) != 0 {
		sqlErrM, err := sqlErrMVar.GetValue(0)
		if err != nil {
			return "", err
		}
		sqlErrTrace, err := sqlErrTraceVar.GetValue(0)
		if err != nil {
			return "", err
		}
		return "", oracle.NewErrorAt(int(sqlErrCode.(int32)), sqlErrM.(string), sqlErrTrace.(string))
	}
	ret, e := retNameVar.GetValue(0)
	if e != nil {
		return "", err
	}

	r.setStepInfo(stepName, stepStm, stepStmParams, true)
	return ret.(string), nil
}

func (r *oracleTasker) openStep(stepType string) string {
	r.Lock()
	defer r.Unlock()
	step := &oracleTaskerStep{stepBg: time.Now(), stepSuccess: false}
	stepName := fmt.Sprintf("%03d - %s", len(r.logCurr.steps)+1, stepType)
	r.logCurr.steps[stepName] = step
	return stepName
}

func (r *oracleTasker) closeStep(stepName string) {
	r.Lock()
	defer r.Unlock()
	step := r.logCurr.steps[stepName]
	step.stepFn = time.Now()
}
func (r *oracleTasker) setStepInfo(stepName, stepStm string, stepParams map[string]interface{}, stepSuccess bool) {
	r.Lock()
	defer r.Unlock()
	step := r.logCurr.steps[stepName]
	step.stepStm = stepStm
	step.stepStmParams = stepParams
	step.stepSuccess = stepSuccess
}

func (r *oracleTasker) Break() error {
	r.Lock()
	defer r.Unlock()
	// Прерываем выполнение текущей сессии.
	// Используем параметры сохраненные подключения
	if !r.stateIsWorking {
		//Выполнение уже завершилось. Нечего прерывать
		return nil
	}
	if r.conn == nil {
		//Выполнение еще не начиналось. Нечего прерывать
		return nil
	}
	if !r.conn.IsConnected() {
		//Выполнение еще не начиналось. Нечего прерывать
		return nil
	}

	if r.sessID == "" {
		return errgo.Newf("Отсутствует информация о сессии")
	}

	return killSession(r.stmKillSession, r.connUserName, r.connUserPass, r.connStr, r.sessID)
}

func killSession(stm, username, password, sid, sessionID string) error {
	conn, err := oracle.NewConnection(username, password, sid, false)
	if err != nil {
		return err
	}
	// Соединение с БД прошло успешно.
	defer conn.Close()

	cur := conn.NewCursor()
	defer cur.Close()

	sesVar, err := cur.NewVariable(0, oracle.StringVarType, 40)
	if err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "sessId", sessionID, err)
	}
	sesVar.SetValue(0, sessionID)

	retMsg, err := cur.NewVariable(0, oracle.StringVarType, 32767)
	if err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "retMsg", "varchar2", err)
	}
	retVar, err := cur.NewVariable(0, oracle.Int32VarType, 0)
	if err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "retVar", "number", err)
	}

	if err := cur.Execute(stm, nil, map[string]interface{}{"sess_id": sesVar, "ret": retVar, "out_err_msg": retMsg}); err != nil {
		return err
	}

	ret, err := retVar.GetValue(0)
	if err != nil {
		return err
	}
	if ret.(int32) != 1 {
		msg, err := retMsg.GetValue(0)
		if err != nil {
			return err
		}
		return errgo.New(msg.(string))
	}

	return nil
}

func (r *oracleTasker) dumpError(userName, connStr, dumpErrorFileName string, err error) {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Имя пользователя : %s\n", userName))
	buf.WriteString(fmt.Sprintf("Строка соединения : %s\n", connStr))
	buf.WriteString(fmt.Sprintf("Дата и время возникновения : %s\n", time.Now().Format(time.RFC1123Z)))
	buf.WriteString("******* Текст SQL  ********************************\n")
	buf.WriteString(r.lastStm() + "\n")
	buf.WriteString("******* Текст SQL закончен ************************\n")
	buf.WriteString("\n")
	buf.WriteString("******* Текст ошибки  *****************************\n")
	buf.WriteString(err.Error() + "\n")
	buf.WriteString("******* Текст ошибки закончен *********************\n")
	dir, _ := filepath.Split(dumpErrorFileName)
	os.MkdirAll(dir, os.ModeDir)
	ioutil.WriteFile(dumpErrorFileName, buf.Bytes(), 0644)
}

func (r *oracleTasker) lastStm() string {
	r.Lock()
	defer r.Unlock()
	var log oracleTaskerMessageLog
	currLog := r.logCurr
	if currLog == nil {
		log = r.logs[len(r.logs)-1]
	} else {
		log = *currLog
	}

	var keys []string
	for k := range log.steps {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return ""
	}

	sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	return log.steps[keys[0]].stepStm
}

func packError(err error) (int, []byte) {
	oraErr := UnMask(err)
	if oraErr != nil {
		switch {
		case oraErr.Code == 28:
			return StatusRequestWasInterrupted, []byte("")
		case oraErr.Code == 31:
			return StatusRequestWasInterrupted, []byte("")
		case oraErr.Code == 1017:
			return StatusInvalidUsernameOrPassword, []byte("")
		case oraErr.Code == 1031:
			return StatusInsufficientPrivileges, []byte("")
		case oraErr.Code == 28000:
			return StatusAccountIsLocked, []byte("")
		case oraErr.Code == 6564:
			return http.StatusNotFound, []byte("")
		default:
			return StatusErrorPage, []byte(errgo.Mask(err).Error())
		}
	}
	return StatusErrorPage, []byte(errgo.Mask(err).Error())

}

type sesStep struct {
	Name      string
	Duration  int32
	Statement string
}

type OracleTaskInfo struct {
	HandlerID        string
	MessageID        string
	Database         string
	UserName         string
	Password         string
	SessionID        string
	Created          string
	RequestProceeded int32
	IdleTime         int32
	LastDuration     int32
	LastSteps        map[int]sesStep
	StepNum          int32
	LastStatement    string
	LastDocument     string
	LastProcedure    string
	NowInProcess     bool
	//History map[int]
}

func (r *oracleTasker) Info() OracleTaskInfo {
	r.Lock()
	defer r.Unlock()
	var log oracleTaskerMessageLog
	currLog := r.logCurr
	if currLog == nil {
		log = r.logs[len(r.logs)-1]
	} else {
		log = *currLog
	}

	processTime := int32(0)
	sSteps := make(map[int]sesStep)

	var keys []string
	for k := range log.steps {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	i := 1
	stm := ""
	for _, v := range keys {
		val := log.steps[v]
		stepTime := int32(0)
		if (val.stepFn == time.Time{}) {
			stepTime = int32(time.Since(val.stepBg) / time.Millisecond)
		} else {
			stepTime = int32(val.stepFn.Sub(val.stepBg) / time.Millisecond)
		}
		processTime = processTime + stepTime
		sSteps[i] = sesStep{Name: v, Duration: stepTime, Statement: val.stepStm}
		stm = val.stepStm
		i = i + 1
	}

	idleTime := int32(time.Since(r.stateLastFinishDT) / time.Millisecond)
	if (r.stateLastFinishDT == time.Time{}) {
		idleTime = 0
	}

	//fmt.Println(log.task.sessionId, " ", int32(len(sSteps)+1))

	return OracleTaskInfo{log.sessionID,
		log.taskID,
		log.connStr,
		log.userName,
		log.userPass,
		r.sessID,
		r.stateCreateDT.Format(time.RFC3339),
		int32(len(r.logs)),
		idleTime,
		processTime,
		sSteps,
		int32(len(sSteps) + 1),
		stm,
		//html.EscapeString(stm),
		log.procName,
		log.procName,
		r.stateIsWorking,
	}
}

func prepareParam(cur *oracle.Cursor, paramName string, paramValue []string, paramDataType, paramSubDataType int32,
	paramStoreProc string, params map[string]interface{}, paramsForCall, paramsForStore *string) error {
	var (
		lVar *oracle.Variable
		err  error
	)
	switch paramDataType {
	case otVarchar2, otString, otPLSQLString,
		otNumber, otFloat, otInteger:
		{
			value := paramValue[0]
			if lVar, err = cur.NewVar(&value); err != nil {
				return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
			}
			params[paramName] = lVar
			*paramsForCall = concat(*paramsForCall, paramName+"=>:"+paramName, ", ")

			// Добавление вызова сохранения параметра
			if paramStoreProc != "" {
				if lVar, err = cur.NewVar(&value); err != nil {
					return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
				}
				params[paramName+"_s"] = lVar
				*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', :%s_s);\n", paramStoreProc, paramName, paramName)
			}
			return nil
		}
	case otBoolean:
		{
			value := paramValue[0]
			*paramsForCall = concat(*paramsForCall, paramName+"=>"+value, ", ")
			// Добавление вызова сохранения параметра
			if paramStoreProc != "" {
				*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', '%s');\n", paramStoreProc, paramName, value)
			}
			return nil
		}
	case otPLSQLIndexByTableType:
		{
			value := make([]interface{}, len(paramValue))
			valueMaxLen := 0
			for i, val := range paramValue {
				value[i] = val
				if len(val) > valueMaxLen {
					valueMaxLen = len(val)
				}
			}

			switch paramSubDataType {
			case otVarchar2, otString, otPLSQLString:
				{

					if lVar, err = cur.NewArrayVar(oracle.StringVarType, value, uint(valueMaxLen)); err != nil {
						return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
					}
					params[paramName] = lVar
					*paramsForCall = concat(*paramsForCall, paramName+"=>:"+paramName, ", ")
					// Добавление вызова сохранения параметра
					if paramStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.StringVarType, value, uint(valueMaxLen)); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar
						for i := range paramValue {
							*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', :%s_s(%d));\n", paramStoreProc, paramName, paramName, i+1)
						}

					}
				}
			case otNumber, otFloat:
				{
					if lVar, err = cur.NewArrayVar(oracle.FloatVarType, value, 0); err != nil {
						return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
					}
					params[paramName] = lVar
					*paramsForCall = concat(*paramsForCall, paramName+"=>:"+paramName, ", ")
					// Добавление вызова сохранения параметра
					if paramStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.FloatVarType, (value), 0); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar
						for i := range paramValue {
							*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', :%s_s(%d));\n", paramStoreProc, paramName, paramName, i+1)
						}
					}
				}
			case otInteger:
				{
					if lVar, err = cur.NewArrayVar(oracle.Int64VarType, value, 0); err != nil {
						return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
					}
					params[paramName] = lVar
					*paramsForCall = concat(*paramsForCall, paramName+"=>:"+paramName, ", ")
					// Добавление вызова сохранения параметра
					if paramStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.Int64VarType, (value), 0); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar
						for i := range paramValue {
							*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', :%s_s(%d));\n", paramStoreProc, paramName, paramName, i+1)
						}
					}
				}
			default:
				{
					return errgo.Newf("error creating variable for %s(%T): Invalid subtype %d", paramName, value, paramSubDataType)
				}
			}
			return nil
		}
		//	case -1:
		//		{
		//			// Параметры запроса, которых нет среди параметров процедуры ТОЛЬКО сохраняем в сессии, но не передаем в процедуру
		//			value := paramValue[0]
		//			if paramStoreProc != "" {
		//				if paramStoreVar, err = cur.NewVar(&value); err != nil {
		//					return nil, nil, "", "", "", "", errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
		//				}
		//				fmt.Println(paramName, " => ", paramStoreVar)
		//				paramStoreVarSql = fmt.Sprintf("  %s('%s', :%s_s);\n", paramStoreProc, paramName, paramName)
		//			}
		//			return nil, paramStoreVar, "", paramStoreVarSql, paramName, paramName + "_s", nil
		//		}
	}
	return nil
}
func concat(str1, str2, delim string) string {
	if str1 == "" {
		return str2
	}
	if str2 == "" {
		return str1
	}
	return str1 + delim + str2
}

func UnMask(err error) *oracle.Error {
	oraErr, ok := err.(*oracle.Error)
	if ok {
		return oraErr
	}
	if errg, ok := err.(*errgo.Err); ok {
		return UnMask(errg.Underlying())
	}
	return nil
}

func ExtractFileName(contentDisposition string) string {
	r := ""
	for _, v := range strings.Split(contentDisposition, "; ") {
		if strings.HasPrefix(v, "filename=") {
			_, r = filepath.Split(strings.Replace(strings.Replace(v, "filename=\"", "", -1), "\"", "", -1))
			return r
		}
	}
	return r
}
