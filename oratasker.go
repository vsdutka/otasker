// taskerOra
package otasker

import (
	"bytes"
	"fmt"
	"gopkg.in/errgo.v1"
	"gopkg.in/goracle.v1/oracle"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	statusErrorPage                 = 561
	statusWaitPage                  = 562
	statusBreakPage                 = 563
	statusRequestWasInterrupted     = 564
	statusInvalidUsernameOrPassword = 565
	statusInsufficientPrivileges    = 566
	statusAccountIsLocked           = 567
)

type sessionTasker interface {
	Run(task *sessionTask)
	Close() error
	Break() error
	Info() sesInfo
}

type sessionTask struct {
	sessionID         string
	taskID            string
	reqUserName       string
	reqUserPass       string
	reqDumpStatements bool
	reqSID            string
	reqParamStoreProc string
	reqBeforeScript   string
	reqAfterScript    string
	reqDocumentTable  string
	reqCGIEnv         map[string]string
	reqProc           string
	reqParams         url.Values
	reqFiles          *multipart.Form
	resDuration       int64
	resStatusCode     int
	resContentType    string
	resHeaders        string
	resContent        []byte
}

type oracleTaskerStep struct {
	stepBg        time.Time
	stepFn        time.Time
	stepStm       string
	stepStmParams map[string]interface{}
	stepResults   string
	stepSuccess   bool
}

type oracleTaskerMessageLog struct {
	task  sessionTask
	steps map[string]*oracleTaskerStep
}

type oracleTasker struct {
	sync.Mutex
	sendOp            func(op *operation)
	streamID          string
	conn              *oracle.Connection
	descr             OracleDescriber
	connUserName      string
	connUserPass      string
	connSID           string
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

func newOracleProcTasker(f func(op *operation), stmEvalSessionID, stmMain, stmGetRestChunk, stmKillSession, stmFileUpload, streamID string) sessionTasker {
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

func (r *oracleTasker) Close() error {
	r.Lock()
	defer r.Unlock()
	r.descr.Clear()
	r.descr = nil
	if r.conn != nil {
		bg := time.Now()
		if err := r.conn.Close(); err != nil {
			logoutOp(r.sendOp, r.streamID, bg, time.Now(), false)
			return err
		}
		logoutOp(r.sendOp, r.streamID, bg, time.Now(), true)
	}
	r.logCurr = nil
	r.logs = nil
	return nil
}

func (r *oracleTasker) Run(task *sessionTask) {
	var bgMem runtime.MemStats
	runtime.ReadMemStats(&bgMem)

	r.Lock()
	r.stateIsWorking = true
	r.logCurr = &oracleTaskerMessageLog{task: *task, steps: make(map[string]*oracleTaskerStep)}
	r.logs = append(r.logs, *r.logCurr)

	defer func(bgMem *runtime.MemStats) {

		r.stateIsWorking = false
		r.stateLastFinishDT = time.Now()
		r.logCurr = nil
		r.Unlock()

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

	if err := r.connect(task.reqUserName, task.reqUserPass, task.reqSID); err != nil {
		task.resStatusCode, task.resContent = packError(err)
		if task.resStatusCode == statusRequestWasInterrupted {
			r.conn.Close()
		}
		r.dumpError(task, err)
		return
	}

	if err := r.run(task); err != nil {
		task.resStatusCode, task.resContent = packError(err)
		if task.resStatusCode == statusRequestWasInterrupted {
			r.conn.Close()
		}
		r.dumpError(task, err)
		return
	}
	task.resStatusCode = http.StatusOK
}

func (r *oracleTasker) connect(username, userpass, connstr string) (err error) {
	const (
		stepName = "001 - connect"
	)
	step := r.openStep(stepName)
	defer r.closeStep(stepName)
	step.stepStm = "connect"
	if (r.conn == nil) || (r.connUserName != username) || (r.connUserPass != userpass) || (r.connSID != connstr) {
		if r.conn != nil {
			bg := time.Now()
			err = r.conn.Close()
			if err != nil {
				logoutOp(r.sendOp, r.streamID, bg, time.Now(), false)
				r.conn = nil
				return err
			}
			logoutOp(r.sendOp, r.streamID, bg, time.Now(), true)
		}
		bg := time.Now()
		r.conn, err = oracle.NewConnection(username, userpass, connstr, false)
		if err != nil {
			loginOp(r.sendOp, r.streamID, bg, time.Now(), false)
			r.conn = nil
			return err
		}
		loginOp(r.sendOp, r.streamID, bg, time.Now(), true)
		r.connUserName = username
		r.connUserPass = userpass
		r.connSID = connstr
		// Соединение с БД прошло успешно.
		if err = r.evalSessionID(); err != nil {
			return err
		}

	} else {
		if !r.conn.IsConnected() {
			bg := time.Now()
			if err = r.conn.Connect(0, false); err != nil {
				loginOp(r.sendOp, r.streamID, bg, time.Now(), false)
				r.conn = nil
				return err
			}
			loginOp(r.sendOp, r.streamID, bg, time.Now(), true)
			if err = r.evalSessionID(); err != nil {
				return err
			}
		}
	}
	step.stepSuccess = true
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
		execOp(r.sendOp, streamID, stm, params, bg, time.Now(), false)
		return err
	}
	execOp(r.sendOp, streamID, stm, params, bg, time.Now(), true)
	return nil
}

func (r *oracleTasker) evalSessionID() error {
	const (
		stepName = "002 - evalSessionID"
	)
	step := r.openStep(stepName)

	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepName) }()

	step.stepStm = r.stmEvalSessionID

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

	step.stepStmParams = map[string]interface{}{"sid": v}

	err = r.execStm(cur, r.streamID, step.stepStm, step.stepStmParams)

	r.sessID = sessID
	step.stepSuccess = err == nil
	return err
}

func (r *oracleTasker) run(task *sessionTask) error {
	const (
		stepName      = "004 - run"
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
	step := r.openStep(stepName)
	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepName) }()

	numParams := int64(len(task.reqCGIEnv))
	var (
		paramName       []interface{}
		paramVal        []interface{}
		paramNameMaxLen int
		paramValMaxLen  int
	)

	for key, val := range task.reqCGIEnv {
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

	step.stepStm = "Describe " + task.reqProc
	dp, err := r.descr.Describe(r, r.conn, task.reqProc)
	if err != nil {
		return err
	}

	var (
		extParamName        []interface{}
		extParamValue       []interface{}
		extParamNameMaxLen  int
		extParamValueMaxLen int
	)
	for paramName, paramValue := range task.reqParams {
		//fmt.Println(paramName, " ", paramValue, " ", dp.ParamDataType(paramName), " ", dp.ParamDataSubType(paramName))
		err := prepareParam(cur, paramName, paramValue,
			dp.ParamDataType(paramName), dp.ParamDataSubType(paramName),
			task.reqParamStoreProc, sqlParams, &sParams, &sParamStore)
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

	if task.reqFiles != nil {
		for paramName, paramValue := range task.reqFiles.File {
			fileName, err := r.saveFile(task, paramValue)
			if err != nil {
				return err
			}
			err = prepareParam(cur, paramName, fileName,
				dp.ParamDataType(paramName), dp.ParamDataSubType(paramName),
				task.reqParamStoreProc, sqlParams, &sParams, &sParamStore)
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

	step.stepStm = fmt.Sprintf(r.stmMain, "", stmParamsInit, task.reqBeforeScript, sParamStore, task.reqProc, sParams, task.reqAfterScript)
	step.stepStmParams = sqlParams

	//fmt.Println(step.stepStm)
	//	if task.reqDumpStatements {
	//		r.dumpStm(task, fmt.Sprintf(r.stmMain, "", "%s", task.reqBeforeScript, sParamStore, task.reqProc, sParams, task.reqAfterScript), sqlParams)
	//	}

	if err := r.execStm(cur, r.streamID, step.stepStm, step.stepStmParams); err != nil {
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
	task.resContentType = contentType

	ch, err := CustomHeadersVar.GetValue(0)
	if err != nil {
		return err
	}
	if ch != nil {
		task.resHeaders = ch.(string)
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
			task.resContent = append(task.resContent, []byte(data.(string))...)
			bNextChunkExists, err := bNextChunkExistsVar.GetValue(0)
			if err != nil {
				return err
			}
			if bNextChunkExists.(int32) != 0 {
				r.getRestChunks(task)
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
					task.resContent = append(task.resContent, buf...)
				}
			}

		}
	}

	step.stepSuccess = true
	return nil
}

func (r *oracleTasker) getRestChunks(task *sessionTask) error {
	const (
		stepName = "005 - getRestChunks"
	)
	var (
		err                 error
		bNextChunkExists    int64
		DataVar             *oracle.Variable
		bNextChunkExistsVar *oracle.Variable
		sqlErrCodeVar       *oracle.Variable
		sqlErrMVar          *oracle.Variable
		sqlErrTraceVar      *oracle.Variable
	)
	step := r.openStep(stepName)
	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepName) }()

	step.stepStm = r.stmGetRestChunk
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

	step.stepStmParams = map[string]interface{}{
		"Data":             DataVar,
		"bNextChunkExists": bNextChunkExistsVar,
		"sqlerrcode":       sqlErrCodeVar,
		"sqlerrm":          sqlErrMVar,
		"sqlerrtrace":      sqlErrTraceVar,
	}

	bNextChunkExists = 1

	for bNextChunkExists != 0 {
		//		if task.reqDumpStatements {
		//			r.dumpStm(task, r.stmGetRestChunk, nil)
		//		}

		if err := r.execStm(cur, r.streamID, step.stepStm, step.stepStmParams); err != nil {
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
		task.resContent = append(task.resContent, []byte(data.(string))...)
	}
	step.stepSuccess = true
	return nil
}

func (r *oracleTasker) saveFile(task *sessionTask, fileHeaders []*FileHeader) ([]string, error) {
	fileNames := make([]string, len(fileHeaders))
	for i, fileHeader := range fileHeaders {
		//_, fileNames[i] = filepath.Split(fileHeader.Filename)
		fileNames[i] = extractFileName(fileHeader.Header.Get("Content-Disposition"))

		fileReader, err := fileHeader.Open()
		if err != nil {
			return nil, err
		}
		fileContent, err := ioutil.ReadAll(fileReader)
		if err != nil {
			return nil, err
		}
		_ = fileContent
		fileContentType := fileHeader.Header.Get("Content-Type")
		fileNames[i], err = r.saveFileToDB(task, fileNames[i], fileHeader.lastArg, fileContentType, fileContentType, fileContent)
		if err != nil {
			return nil, err
		}
	}
	return fileNames, nil
}

func (r *oracleTasker) saveFileToDB(task *sessionTask, fName, fItem, fMime, fContentType string, fContent []byte) (string, error) {
	const (
		stepName = "003 - saveFileToDB"
		//		stmParamsInit = `
		//  l_num_params     := :num_params;
		//  l_param_name     := :param_name;
		//  l_param_val      := :param_val;
		//  l_item_id        := :item_id;
		//  l_application_id := :application_id;
		//  l_page_id        := :page_id;
		//  l_session_id     := :session_id;
		//  l_request        := :request;
		//  l_doc_size       := :doc_size;
		//  l_content_type   := :content_type;
		//`
	)
	step := r.openStep(stepName)
	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepName) }()

	numParams := int64(len(task.reqCGIEnv))
	var (
		paramName       []interface{}
		paramVal        []interface{}
		paramNameMaxLen int
		paramValMaxLen  int
	)

	for key, val := range task.reqCGIEnv {
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

	applicationID := task.reqParams.Get("p_flow_id")
	pageID := task.reqParams.Get("p_flow_step_id")
	sessionID := task.reqParams.Get("p_instance")
	request := task.reqParams.Get("p_request")

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
	//fmt.Println(fmt.Sprintf(r.stmFileUpload, task.reqBeforeScript, task.documentTable))

	step.stepStm = fmt.Sprintf(r.stmFileUpload, task.reqBeforeScript, task.reqDocumentTable)
	step.stepStmParams = map[string]interface{}{"num_params": numParamsVar,
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

	//	if task.reqDumpStatements {
	//		r.dumpStm(task, step.stepStm, step.stepStmParams)
	//	}
	if err := r.execStm(cur, r.streamID, step.stepStm, step.stepStmParams); err != nil {
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

	step.stepSuccess = true
	return ret.(string), nil
}

func (r *oracleTasker) openStep(stepName string) *oracleTaskerStep {
	step := &oracleTaskerStep{stepBg: time.Now(), stepSuccess: false}
	r.logCurr.steps[stepName] = step
	return step
}

func (r *oracleTasker) closeStep(stepName string) {
	step := r.logCurr.steps[stepName]
	step.stepFn = time.Now()
}

func (r *oracleTasker) Break() error {
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

	return killSession(r.stmKillSession, r.connUserName, r.connUserPass, r.connSID, r.sessID)
	//	conn, err := oracle.NewConnection(r.connUserName, r.connUserPass, r.connSID, false)
	//	if err != nil {
	//		return err
	//	}
	//	// Соединение с БД прошло успешно.
	//	defer conn.Close()

	//	cur := conn.NewCursor()
	//	defer cur.Close()

	//	sesVar, err := cur.NewVariable(0, oracle.StringVarType, 40)
	//	if err != nil {
	//		return errgo.Newf("error creating variable for %s(%T): %s", "sessId", r.sessID, err)
	//	}
	//	sesVar.SetValue(0, r.sessID)

	//	fmt.Println("r.sessID = ", r.sessID)
	//	retMsg, err := cur.NewVariable(0, oracle.StringVarType, 32767)
	//	if err != nil {
	//		return errgo.Newf("error creating variable for %s(%T): %s", "retMsg", "varchar2", err)
	//	}
	//	retVar, err := cur.NewVariable(0, oracle.Int32VarType, 0)
	//	if err != nil {
	//		return errgo.Newf("error creating variable for %s(%T): %s", "retVar", "number", err)
	//	}

	//	if err := cur.Execute(r.stmKillSession, nil, map[string]interface{}{"sess_id": sesVar, "ret": retVar, "out_err_msg": retMsg}); err != nil {
	//		return err
	//	}

	//	ret, err := retVar.GetValue(0)
	//	if err != nil {
	//		return err
	//	}
	//	if ret.(int32) != 1 {
	//		msg, err := retMsg.GetValue(0)
	//		if err != nil {
	//			return err
	//		}
	//		return errgo.New(msg.(string))
	//	}

	//	return nil
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

	fmt.Println("r.sessID = ", sessionID)
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

func (r *oracleTasker) dumpError(task *sessionTask, err error) {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Имя пользователя : %s\n", task.reqUserName))
	buf.WriteString(fmt.Sprintf("Строка соединения : %s\n", task.reqSID))
	buf.WriteString(fmt.Sprintf("Дата и время возникновения : %s\n", time.Now().Format(time.RFC1123Z)))
	buf.WriteString("******* Текст SQL  ********************************\n")
	buf.WriteString(r.lastStm() + "\n")
	buf.WriteString("******* Текст SQL закончен ************************\n")
	buf.WriteString("\n")
	buf.WriteString("******* Текст ошибки  *****************************\n")
	buf.WriteString(err.Error() + "\n")
	buf.WriteString("******* Текст ошибки закончен *********************\n")
	srv.writeTraceFile(fmt.Sprintf("${log_dir}\\err_%s_${datetime}_(%s).log", task.reqUserName, r.sessID), buf.String())
}

//func (r *oracleSessionTasker) dumpStm(task *sessionTask, stm string, sqlParams map[string]interface{}) {
//	var buf bytes.Buffer

//	if sqlParams != nil {

//		for k, v := range sqlParams {
//			if !(k == "ContentType" || k == "ContentLength" || k == "CustomHeaders" || k == "rc__" || k == "content__" || k == "lob__" || k == "bNextChunkExists") {
//				name := k
//				if !(k == "num_params" || k == "param_name" || k == "param_val" || k == "num_ext_params" || k == "ext_param_name" || k == "ext_param_val" || k == "package_name") {
//					name = "  :" + name
//				} else {
//					name = "  l_" + name
//				}

//				v1 := v.(*oracle.Variable)
//				if v1.ArrayLength() > 0 {
//					for i := uint(1); i < v1.ArrayLength()+1; i++ {
//						v2, _ := v1.GetValue(i - 1)
//						rval := reflect.ValueOf(v2)
//						switch rval.Kind() {
//						case reflect.String:
//							buf.WriteString(fmt.Sprintf("%s(%d) := '%s';\n", name, i, v2.(string)))
//						case reflect.Int:
//							buf.WriteString(fmt.Sprintf("%s(%d) := %v;\n", name, i, v2.(int)))
//						case reflect.Int32:
//							buf.WriteString(fmt.Sprintf("%s(%d) := %v;\n", name, i, v2.(int32)))
//						case reflect.Int64:
//							buf.WriteString(fmt.Sprintf("%s(%d) := %v;\n", name, i, v2.(int64)))
//						case reflect.Ptr:

//						default:
//							log.Println(k, " ", rval.Kind())
//							panic(rval.Kind())
//						}
//					}
//				} else {
//					v2, _ := v1.GetValue(0)
//					rval := reflect.ValueOf(v2)
//					switch rval.Kind() {
//					case reflect.String:
//						buf.WriteString(fmt.Sprintf("%s := '%s';\n", name, v2.(string)))
//					case reflect.Int:
//						buf.WriteString(fmt.Sprintf("%s := %v;\n", name, v2.(int)))
//					case reflect.Int32:
//						buf.WriteString(fmt.Sprintf("%s := %v;\n", name, v2.(int32)))
//					case reflect.Int64:
//						buf.WriteString(fmt.Sprintf("%s := %v;\n", name, v2.(int64)))
//					case reflect.Ptr:
//					default:
//						log.Println(k, " ", rval.Kind())
//						panic(rval.Kind())
//					}
//				}
//			}
//		}
//	}
//	srv.writeTraceFile(fmt.Sprintf("${log_dir}\\trace_%s_(%s)_${datetime}.log", task.reqUserName, r.sessID), fmt.Sprintf(stm, buf.String()))
//}

func (r *oracleTasker) lastStm() string {
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
	oraErr := unMask(err)
	if oraErr != nil {
		switch {
		case oraErr.Code == 28:
			return statusRequestWasInterrupted, []byte("")
		case oraErr.Code == 31:
			return statusRequestWasInterrupted, []byte("")
		case oraErr.Code == 1017:
			return statusInvalidUsernameOrPassword, []byte("")
		case oraErr.Code == 1031:
			return statusInsufficientPrivileges, []byte("")
		case oraErr.Code == 28000:
			return statusAccountIsLocked, []byte("")
		case oraErr.Code == 6564:
			return http.StatusNotFound, []byte("")
		default:
			return statusErrorPage, []byte(errgo.Mask(err).Error())
		}
	}
	return statusErrorPage, []byte(errgo.Mask(err).Error())

}

type sesStep struct {
	Name      string
	Duration  int32
	Statement string
}

type sesInfo struct {
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

type sessionsInfo struct {
	Sessions []sesInfo
}

func (r *oracleTasker) Info() sesInfo {
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

	return sesInfo{log.task.sessionID,
		log.task.taskID,
		log.task.reqSID,
		log.task.reqUserName,
		log.task.reqUserPass,
		r.sessID,
		r.stateCreateDT.Format(time.RFC3339),
		int32(len(r.logs)),
		idleTime,
		processTime,
		sSteps,
		int32(len(sSteps) + 1),
		stm,
		//html.EscapeString(stm),
		log.task.reqProc,
		log.task.reqProc,
		r.stateIsWorking,
	}
}

//func (r *oracleSessionTasker) Test() {
//	const (
//		stm = `
//begin
//  raise_application_error(-20001, 'test error');;
//end;
//`
//	)

//	err := func() error {
//		conn, err := oracle.NewConnection(r.connUserName, r.connUserPass, r.connSID, false)
//		if err != nil {
//			fmt.Println(err.(*errgo.Err).Underlying().(*oracle.Error))
//			oraErr, ok := err.(*errgo.Err).Underlying().(*oracle.Error)
//			if ok {
//				fmt.Println("oraErrCode=", oraErr.Code)
//			} else {
//				fmt.Println("not ok")
//			}
//			return err
//		}
//		// Соединение с БД прошло успешно.
//		defer conn.Close()

//		cur := conn.NewCursor()
//		defer cur.Close()

//		return cur.Execute(stm, nil, nil)
//	}()

//	fmt.Println(err)
//	oraErr, ok := err.(*oracle.Error)
//	if ok {
//		fmt.Println(oraErr.Code)
//	} else {
//		fmt.Println("not ok")
//	}
//}

//func UnMask(err error) *oracle.Error {
//	oraErr, ok := err.(*oracle.Error)
//	if ok {
//		return oraErr
//	}
//	oraErr, ok = err.(*errgo.Err).Underlying().(*oracle.Error)
//	if ok {
//		return oraErr
//	}
//	oraErr, ok = err.(*errgo.Err).Underlying().(*errgo.Err).Underlying().(*oracle.Error)
//	if ok {
//		return oraErr
//	}
//	return nil
//}

//func extractFileName(contentDisposition string) string {
//	r := ""
//	for _, v := range strings.Split(contentDisposition, "; ") {
//		if strings.HasPrefix(v, "filename=") {
//			_, r = filepath.Split(strings.Replace(strings.Replace(v, "filename=\"", "", -1), "\"", "", -1))
//			return r
//		}
//	}
//	return r
//}

func prepareParam(cur *oracle.Cursor, paramName string, paramValue []string, paramDataType, paramSubDataType int32,
	reqParamStoreProc string, params map[string]interface{}, paramsForCall, paramsForStore *string) error {
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
			if reqParamStoreProc != "" {
				if lVar, err = cur.NewVar(&value); err != nil {
					return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
				}
				params[paramName+"_s"] = lVar
				*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', :%s_s);\n", reqParamStoreProc, paramName, paramName)
			}
			return nil
		}
	case otBoolean:
		{
			value := paramValue[0]
			*paramsForCall = concat(*paramsForCall, paramName+"=>"+value, ", ")
			// Добавление вызова сохранения параметра
			if reqParamStoreProc != "" {
				*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', '%s');\n", reqParamStoreProc, paramName, value)
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
					if reqParamStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.StringVarType, value, uint(valueMaxLen)); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar
						for i, _ := range paramValue {
							*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', :%s_s(%d));\n", reqParamStoreProc, paramName, paramName, i+1)
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
					if reqParamStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.FloatVarType, (value), 0); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar
						for i, _ := range paramValue {
							*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', :%s_s(%d));\n", reqParamStoreProc, paramName, paramName, i+1)
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
					if reqParamStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.Int64VarType, (value), 0); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar
						for i, _ := range paramValue {
							*paramsForStore = *paramsForStore + fmt.Sprintf("  %s('%s', :%s_s(%d));\n", reqParamStoreProc, paramName, paramName, i+1)
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
		//			if reqParamStoreProc != "" {
		//				if paramStoreVar, err = cur.NewVar(&value); err != nil {
		//					return nil, nil, "", "", "", "", errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
		//				}
		//				fmt.Println(paramName, " => ", paramStoreVar)
		//				paramStoreVarSql = fmt.Sprintf("  %s('%s', :%s_s);\n", reqParamStoreProc, paramName, paramName)
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
