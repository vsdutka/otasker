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
	"reflect"
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
	Info(sortKeyName string) OracleTaskInfo
}

type oracleTaskerStep struct {
	stepID             int
	stepName           string
	stepBg             time.Time
	stepFn             time.Time
	stepStm            string
	stepStmForShowning string
	stepSuccess        bool
}

const (
	stepConnectNum = iota
	stepEvalSid
	stepDescribeNum
	stepSaveFileToDBNum
	stepRunNum
	stepChunkGetNum
	stepDisconnectNum
)

type oracleTasker struct {
	sync.Mutex                     //включается только при изменении данных, используемых в Break() и Info()
	cafMutex            sync.Mutex // требуется для синхронизации разрушения объекта C(lose )A(nd )F(ree )Mutex
	opLoggerName        string
	streamID            string
	conn                *oracle.Connection
	connUserName        string
	connUserPass        string
	connStr             string
	sessID              string
	logRequestProceeded int
	logErrorsNum        int
	logSessionID        string
	logTaskID           string
	logUserName         string
	logUserPass         string
	logConnStr          string
	logProcName         string
	logSteps            map[int]*oracleTaskerStep

	stateIsWorking    bool
	stateCreateDT     time.Time
	stateLastFinishDT time.Time
	stmEvalSessionID  string
	stmMain           string
	stmGetRestChunk   string
	stmKillSession    string
	stmFileUpload     string
}

var stepsFree = sync.Pool{
	New: func() interface{} { return new(oracleTaskerStep) },
}

func newOracleProcTasker(stmEvalSessionID, stmMain, stmGetRestChunk, stmKillSession, stmFileUpload string) OracleTasker {
	r := oracleTasker{}
	r.stateIsWorking = false
	r.stateCreateDT = time.Now()
	r.stateLastFinishDT = time.Time{}
	r.logSteps = make(map[int]*oracleTaskerStep)

	r.stmEvalSessionID = stmEvalSessionID
	r.stmMain = stmMain
	r.stmGetRestChunk = stmGetRestChunk
	r.stmKillSession = stmKillSession
	r.stmFileUpload = stmFileUpload
	return &r
}

func (r *oracleTasker) initLog() {
	if r.logSteps != nil {
		for k := range r.logSteps {
			s := r.logSteps[k]
			s.stepName = ""
			s.stepStm = ""
			s.stepStmForShowning = ""
			stepsFree.Put(s)
			delete(r.logSteps, k)
		}
	}
	r.logSessionID = ""
	r.logTaskID = ""
	r.logUserName = ""
	r.logUserPass = ""
	r.logConnStr = ""
	r.logProcName = ""
	r.logSteps = make(map[int]*oracleTaskerStep)
}

func (r *oracleTasker) CloseAndFree() error {
	r.cafMutex.Lock()
	defer r.cafMutex.Unlock()

	if r.conn != nil {
		if err := r.conn.Close(); err != nil {
			return err
		}
	}
	r.initLog()

	return nil
}

func (r *oracleTasker) Run(sessionID, taskID, userName, userPass, connStr,
	paramStoreProc, beforeScript, afterScript, documentTable string,
	cgiEnv map[string]string, procName string, urlParams url.Values,
	reqFiles *Form, dumpErrorFileName string) OracleTaskResult {

	r.cafMutex.Lock()

	func() {
		r.Lock()
		defer r.Unlock()
		r.initLog()
		r.logRequestProceeded++
		r.logSessionID = sessionID
		r.logTaskID = taskID
		r.logUserName = userName
		r.logUserPass = userPass
		r.logConnStr = connStr
		r.logProcName = procName
		r.stateIsWorking = true
	}()

	defer func() {

		func() {
			r.Lock()
			defer r.Unlock()
			r.stateIsWorking = false
			r.stateLastFinishDT = time.Now()
		}()

		r.cafMutex.Unlock()
	}()

	bg := time.Now()
	var needDisconnect bool
	var res = OracleTaskResult{}
	if err := r.connect(userName, userPass, connStr); err != nil {
		res.StatusCode, res.Content, needDisconnect = packError(err)
		// Формируем дамп до закрытия соединения, чтобы получить корректный запрос из последнего шага
		r.dumpError(userName, connStr, dumpErrorFileName, err)

		//Если произошла ошибка, всегда закрываем соединение с БД
		r.disconnect()

		res.Duration = int64(time.Since(bg) / time.Second)
		func() {
			r.Lock()
			defer r.Unlock()
			r.logErrorsNum++
		}()
		return res
	}

	if err := r.run(&res, paramStoreProc, beforeScript, afterScript, documentTable,
		cgiEnv, procName, urlParams, reqFiles); err != nil {
		res.StatusCode, res.Content, needDisconnect = packError(err)
		// Формируем дамп до закрытия соединения, чтобы получить корректный запрос из последнего шага
		r.dumpError(userName, connStr, dumpErrorFileName, err)
		if needDisconnect {
			//Если произошла ошибка, всегда закрываем соединение с БД
			r.disconnect()
		}
		res.Duration = int64(time.Since(bg) / time.Second)
		func() {
			r.Lock()
			defer r.Unlock()
			r.logErrorsNum++
		}()
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
			r.openStep(stepConnectNum, "connect")
			defer r.closeStep(stepConnectNum)
			r.setStepInfo(stepConnectNum, "connect", "connect", false)
			r.conn, err = oracle.NewConnection(username, userpass, connstr, false)
			if err != nil {
				// Если выходим с ошибкой, то в вызывающей процедуре будет вызван disconnect()
				return err
			}
			r.connUserName = username
			r.connUserPass = userpass
			r.connStr = connstr
			// Соединение с БД прошло успешно.
			if err = r.evalSessionID(); err != nil {
				// Если выходим с ошибкой, то в вызывающей процедуре будет вызван disconnect()
				return err
			}
			r.setStepInfo(stepConnectNum, "connect", "connect", true)
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
			r.openStep(stepDisconnectNum, "disconnect")
			r.setStepInfo(stepDisconnectNum, "disconnect", "disconnect", false)

			r.Lock()

			defer func() {
				r.conn = nil
				r.connUserName = ""
				r.connUserPass = ""
				r.connStr = ""
				r.sessID = ""
				r.Unlock()
				r.setStepInfo(stepDisconnectNum, "disconnect", "disconnect", true)
				r.closeStep(stepDisconnectNum)

			}()

			if r.conn != nil {
				r.conn.Close()
			}
		} else {
			// Очистка в случае неудачного Logon
			r.conn.Free(true)
		}
	}
	return nil
}

func (r *oracleTasker) evalSessionID() error {
	r.openStep(stepEvalSid, "evalSessionID")

	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepEvalSid) }()

	r.sessID = ""
	stepStm := r.stmEvalSessionID
	r.setStepInfo(stepEvalSid, stepStm, stepStm, false)

	err := cur.Execute(stepStm, nil, nil)
	if err != nil {
		return err
	}
	row, err1 := cur.FetchOne()
	if err1 != nil {
		return err1
	}
	r.sessID = row[0].(string)
	r.setStepInfo(stepEvalSid, stepStm, stepStm, true)
	return err
}

func (r *oracleTasker) run(res *OracleTaskResult, paramStoreProc, beforeScript, afterScript, documentTable string,
	cgiEnv map[string]string, procName string, urlParams url.Values, reqFiles *Form) error {

	const (
		initParams = `
  l_num_params := :num_params;
  l_param_name := :param_name;
  l_param_val := :param_val;
  l_num_ext_params := :num_ext_params;
  l_ext_param_name := :ext_param_name;
  l_ext_param_val := :ext_param_val;
  l_package_name := :package_name;
`
	)
	err := func() error {
		r.openStep(stepDescribeNum, "Describe")
		defer r.closeStep(stepDescribeNum)
		return Describe(r.conn, r.connStr, procName)

	}()
	if err != nil {
		return err
	}

	var (
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
	var (
		stmExecDeclarePart bytes.Buffer
		stmShowDeclarePart bytes.Buffer

		stmExecSetPart bytes.Buffer
		stmShowSetPart bytes.Buffer

		stmExecProcParams     bytes.Buffer
		stmShowProcParams     bytes.Buffer
		stmExecStoreInContext bytes.Buffer
		stmShowStoreInContext bytes.Buffer
	)

	r.openStep(stepRunNum, "run")
	defer r.closeStep(stepRunNum)

	cur := r.conn.NewCursor()
	defer cur.Close()

	stmExecSetPart.WriteString(initParams)

	numParams := int32(len(cgiEnv))

	var (
		paramNameMaxLen int
		paramValMaxLen  int
	)
	for key, val := range cgiEnv {
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

	if paramNameVar, err = cur.NewVariable(uint(numParams), oracle.StringVarType, uint(paramNameMaxLen)); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "paramName", "string", err)
	}

	if paramValVar, err = cur.NewVariable(uint(numParams), oracle.StringVarType, uint(paramValMaxLen)); err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "paramVal", "string", err)
	}

	stmShowSetPart.WriteString(fmt.Sprintf("  l_num_params := %d;\n", numParams))

	i := uint(0)
	for key, val := range cgiEnv {
		paramNameVar.SetValue(i, key)
		paramValVar.SetValue(i, val)

		stmShowSetPart.WriteString(fmt.Sprintf("  l_param_name(%d) := '%s';\n", i+1, key))
		stmShowSetPart.WriteString(fmt.Sprintf("  l_param_val(%d) := '%s';\n", i+1, val))
		i++
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

	var (
		extParamName        []interface{}
		extParamValue       []interface{}
		extParamNameMaxLen  int
		extParamValueMaxLen int
	)
	for paramName, paramValue := range urlParams {
		paramType, paramTypeName, err := ArgumentInfo(r.connStr, procName, paramName)
		if err != nil {
			return err
		}
		err = prepareParam(cur, sqlParams,
			paramName, paramValue,
			paramType, paramTypeName,
			paramStoreProc,
			&stmExecDeclarePart, &stmShowDeclarePart,
			&stmExecSetPart, &stmShowSetPart,
			&stmExecProcParams, &stmShowProcParams,
			&stmExecStoreInContext, &stmShowStoreInContext)
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
			paramType, paramTypeName, err1 := ArgumentInfo(r.connStr, procName, paramName)
			if err1 != nil {
				return err1
			}
			err = prepareParam(cur, sqlParams,
				paramName, fileName,
				paramType, paramTypeName,
				paramStoreProc,
				&stmExecDeclarePart, &stmShowDeclarePart,
				&stmExecSetPart, &stmShowSetPart,
				&stmExecProcParams, &stmShowProcParams,
				&stmExecStoreInContext, &stmShowStoreInContext)
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

	stmExecSetPart.WriteString(fmt.Sprintf("  l_num_ext_params := %d;\n", int32(len(extParamName))))
	stmShowSetPart.WriteString(fmt.Sprintf("  l_num_ext_params := %d;\n", int32(len(extParamName))))
	for key, val := range extParamName {
		//FIXME Заменить текстовую константу на использование bind-переменной
		stmExecSetPart.WriteString(fmt.Sprintf("  l_ext_param_name(%d) := '%s';\n", key+1, val))
		stmShowSetPart.WriteString(fmt.Sprintf("  l_ext_param_name(%d) := '%s';\n", key+1, val))
	}

	for key, val := range extParamValue {
		s, _ := val.(string)
		stmExecSetPart.WriteString(fmt.Sprintf("  l_ext_param_val(%d) := '%s';\n", key+1, strings.Replace(s, "'", "''", -1)))
		stmShowSetPart.WriteString(fmt.Sprintf("  l_ext_param_val(%d) := '%s';\n", key+1, strings.Replace(s, "'", "''", -1)))
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

	var pkgName string
	_, pkgName, err = ProcedureInfo(r.connStr, procName)
	if err != nil {
		return err
	}

	var pnVar *oracle.Variable
	pnVar, err = cur.NewVariable(0, oracle.StringVarType, 80)
	if err != nil {
		return errgo.Newf("error creating variable for %s(%T): %s", "package_name", "varchar2", err)
	}
	pnVar.SetValue(0, pkgName)
	sqlParams["package_name"] = pnVar

	stepStm := fmt.Sprintf(r.stmMain, stmExecDeclarePart.String(), stmExecSetPart.String(), beforeScript, stmExecStoreInContext.String(), procName, stmExecProcParams.String(), afterScript)
	stepStmForShowing := fmt.Sprintf(r.stmMain, stmShowDeclarePart.String(), stmShowSetPart.String(), beforeScript, stmShowStoreInContext.String(), procName, stmShowProcParams.String(), afterScript)
	stepStmParams := sqlParams

	r.setStepInfo(stepRunNum, stepStm, stepStmForShowing, false)

	if err := cur.Execute(stepStm, nil, stepStmParams); err != nil {
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

	r.setStepInfo(stepRunNum, stepStm, stepStmForShowing, true)
	return nil
}

func (r *oracleTasker) getRestChunks(res *OracleTaskResult) error {
	var (
		err                 error
		bNextChunkExists    int32
		DataVar             *oracle.Variable
		bNextChunkExistsVar *oracle.Variable
		sqlErrCodeVar       *oracle.Variable
		sqlErrMVar          *oracle.Variable
		sqlErrTraceVar      *oracle.Variable
	)
	r.openStep(stepChunkGetNum, "getRestChunks")
	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepChunkGetNum) }()

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
	r.setStepInfo(stepChunkGetNum, stepStm, stepStm, true)
	bNextChunkExists = 1

	for bNextChunkExists != 0 {
		if err := cur.Execute(stepStm, nil, stepStmParams); err != nil {
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
	r.setStepInfo(stepChunkGetNum, stepStm, stepStm, true)
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

	r.openStep(stepSaveFileToDBNum, "saveFileToDB")
	cur := r.conn.NewCursor()
	defer func() { cur.Close(); r.closeStep(stepSaveFileToDBNum) }()

	numParams := int32(len(cgiEnv))

	var (
		paramNameMaxLen int
		paramValMaxLen  int
	)
	for key, val := range cgiEnv {
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
	paramNameVar, err := cur.NewVariable(uint(numParams), oracle.StringVarType, uint(paramNameMaxLen))
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", "paramName", "string", err)
	}
	paramValVar, err := cur.NewVariable(uint(numParams), oracle.StringVarType, uint(paramValMaxLen))
	if err != nil {
		return "", errgo.Newf("error creating variable for %s(%T): %s", "paramVal", "string", err)
	}

	i := uint(0)
	for key, val := range cgiEnv {
		paramNameVar.SetValue(i, key)
		paramValVar.SetValue(i, val)
		i++
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

	r.setStepInfo(stepSaveFileToDBNum, stepStm, stepStm, false)

	if err := cur.Execute(stepStm, nil, stepStmParams); err != nil {
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

	r.setStepInfo(stepSaveFileToDBNum, stepStm, stepStm, true)
	return ret.(string), nil
}

func (r *oracleTasker) openStep(stepNum int, stepType string) {
	r.Lock()
	defer r.Unlock()

	stepName := fmt.Sprintf("%03d - %s", stepNum, stepType)
	intf := stepsFree.Get()
	step := intf.(*oracleTaskerStep)
	step.stepID = stepNum
	step.stepName = stepName
	step.stepBg = time.Now()
	step.stepFn = time.Time{}
	step.stepStm = ""
	step.stepSuccess = false
	r.logSteps[stepNum] = step
}

func (r *oracleTasker) closeStep(stepNum int) {
	r.Lock()
	defer r.Unlock()
	step := r.logSteps[stepNum]
	step.stepFn = time.Now()
	r.logSteps[stepNum] = step
}
func (r *oracleTasker) setStepInfo(stepNum int, stepStm, stepStmForShowning string, stepSuccess bool) {
	r.Lock()
	defer r.Unlock()
	step := r.logSteps[stepNum]
	step.stepStm = stepStm
	step.stepStmForShowning = stepStmForShowning
	step.stepSuccess = stepSuccess
	r.logSteps[stepNum] = step
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
	stm, stmShow := r.lastStms()
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Имя пользователя : %s\n", userName))
	buf.WriteString(fmt.Sprintf("Строка соединения : %s\n", connStr))
	buf.WriteString(fmt.Sprintf("Дата и время возникновения : %s\n", time.Now().Format(time.RFC1123Z)))
	buf.WriteString("******* Текст SQL  ********************************\n")
	buf.WriteString(stm + "\n")
	buf.WriteString("******* Текст SQL закончен ************************\n")
	buf.WriteString("\n")
	buf.WriteString("******* Текст ошибки  *****************************\n")
	buf.WriteString(err.Error() + "\n")
	buf.WriteString("******* Текст ошибки закончен *********************\n")
	buf.WriteString("\n")
	buf.WriteString("******* Текст SQL с параметрами *******************\n")
	buf.WriteString(stmShow + "\n")
	buf.WriteString("******* Текст SQL с параметрами закончен **********\n")
	dir, _ := filepath.Split(dumpErrorFileName)
	os.MkdirAll(dir, os.ModeDir)
	ioutil.WriteFile(dumpErrorFileName, buf.Bytes(), 0644)
}

func (r *oracleTasker) lastStms() (string, string) {
	r.Lock()
	defer r.Unlock()

	var keys []int
	for k := range r.logSteps {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return "", ""
	}

	sort.Sort(sort.Reverse(sort.IntSlice(keys)))
	step, ok := r.logSteps[keys[0]]
	if !ok {
		return "", ""
	}
	return step.stepStm, step.stepStmForShowning
}

func packError(err error) (int, []byte, bool) {
	oraErr := UnMask(err)
	if oraErr != nil {
		switch {
		case oraErr.Code == 28:
			return StatusRequestWasInterrupted, []byte(""), true
		case oraErr.Code == 31:
			return StatusRequestWasInterrupted, []byte(""), true
		case oraErr.Code == 1017:
			return StatusInvalidUsernameOrPassword, []byte(""), true
		case oraErr.Code == 1031:
			return StatusInsufficientPrivileges, []byte(""), true

		case oraErr.Code == 28000:
			return StatusAccountIsLocked, []byte(""), true
		case oraErr.Code == 6564:
			return http.StatusNotFound, []byte(""), false
		case oraErr.Code == 3113:
			return StatusErrorPage, []byte(errgo.Mask(err).Error()), true
		default:
			return StatusErrorPage, []byte(errgo.Mask(err).Error()), false
		}
	}
	return StatusErrorPage, []byte(errgo.Mask(err).Error()), false

}

type sesStep struct {
	Name      string
	Duration  int32
	Statement string
}

type OracleTaskInfo struct {
	SortKey          string
	HandlerID        string
	MessageID        string
	Database         string
	UserName         string
	Password         string
	SessionID        string
	Created          string
	RequestProceeded int
	ErrorsNumber     int
	IdleTime         int32
	LastDuration     int32
	LastSteps        map[int]sesStep
	StepNum          int32
	StepName         string
	LastDocument     string
	LastProcedure    string
	NowInProcess     bool
}

type OracleTaskInfos []OracleTaskInfo

func (slice OracleTaskInfos) Len() int {
	return len(slice)
}

func (slice OracleTaskInfos) Less(i, j int) bool {
	return slice[i].SortKey < slice[j].SortKey
}

func (slice OracleTaskInfos) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

func (r *oracleTasker) Info(sortKeyName string) OracleTaskInfo {
	r.Lock()
	defer r.Unlock()

	processTime := int32(0)
	sSteps := make(map[int]sesStep)

	var keys []int
	for k := range r.logSteps {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	i := 1

	stepName := ""
	for _, v := range keys {
		val := r.logSteps[v]
		stepTime := int32(0)
		if (val.stepFn == time.Time{}) {
			stepName = val.stepName
			stepTime = int32(time.Since(val.stepBg) / time.Millisecond)
		} else {
			stepTime = int32(val.stepFn.Sub(val.stepBg) / time.Millisecond)
		}

		processTime = processTime + stepTime
		sSteps[i] = sesStep{Name: val.stepName, Duration: stepTime, Statement: val.stepStmForShowning}

		i = i + 1
	}

	idleTime := int32(time.Since(r.stateLastFinishDT) / time.Millisecond)
	if (r.stateLastFinishDT == time.Time{}) {
		idleTime = 0
	}

	res := OracleTaskInfo{
		"",
		r.logSessionID,
		r.logTaskID,
		r.logConnStr,
		r.logUserName,
		r.logUserPass,
		r.sessID,
		r.stateCreateDT.Format(time.RFC3339),
		r.logRequestProceeded,
		r.logErrorsNum,
		idleTime,
		processTime,
		sSteps,
		int32(len(sSteps) + 1),
		stepName,
		r.logProcName,
		r.logProcName,
		r.stateIsWorking,
	}
	rflct := reflect.ValueOf(res)
	f := reflect.Indirect(rflct).FieldByName(sortKeyName)

	switch k := f.Kind(); k {
	case reflect.Invalid:
		res.SortKey = "<invalid Value>"
	case reflect.String:
		res.SortKey = f.String()
	case reflect.Int, reflect.Int32, reflect.Int64:
		res.SortKey = fmt.Sprintf("%040d", f.Int())
	case reflect.Bool:
		res.SortKey = fmt.Sprintf("%v", f.Bool())
	}

	return res
}

func prepareParam(
	cur *oracle.Cursor, params map[string]interface{},
	paramName string, paramValue []string,
	paramType int32, paramTypeName string,
	paramStoreProc string,
	stmExecDeclarePart, stmShowDeclarePart,
	stmExecSetPart, stmShowSetPart,
	stmExecProcParams, stmShowProcParams,
	stmExecStoreInContext, stmShowStoreInContext *bytes.Buffer,
	/*
		paramsForCall, paramsForStore bytes.Buffer*/) error {
	var (
		lVar *oracle.Variable
		err  error
	)

	switch paramType {
	case oString:
		{
			value := paramValue[0]
			if lVar, err = cur.NewVar(&value); err != nil {
				panic(fmt.Sprintf("1 - %v %v %v", paramName, paramType, paramTypeName))
				return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
			}
			params[paramName] = lVar

			// stmExecDeclarePart
			stmShowDeclarePart.WriteString(fmt.Sprintf("  l_%s %s;\n", paramName, paramTypeName))
			//stmExecSetPart,
			stmShowSetPart.WriteString(fmt.Sprintf("  l_%s := '%s';\n", paramName, strings.Replace(value, "'", "''", -1)))
			// Вызов процедуры - Формирование строки с параметрами для вызова процедуры
			if stmExecProcParams.Len() != 0 {
				stmExecProcParams.WriteString(", ")
			}
			stmExecProcParams.WriteString(fmt.Sprintf("%s => :%s", paramName, paramName))

			// Отображение вызова процедуры - Формирование строки с параметрами для вызова процедуры
			if stmShowProcParams.Len() != 0 {
				stmShowProcParams.WriteString(", ")
			}
			stmShowProcParams.WriteString(fmt.Sprintf("%s => l_%s", paramName, paramName))

			// Добавление вызова сохранения параметра
			if paramStoreProc != "" {
				if lVar, err = cur.NewVar(&value); err != nil {
					return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
				}
				params[paramName+"_s"] = lVar
				stmExecStoreInContext.WriteString(fmt.Sprintf("  %s('%s', :%s_s);\n", paramStoreProc, paramName, paramName))
				stmShowStoreInContext.WriteString(fmt.Sprintf("  %s('%s', l_%s);\n", paramStoreProc, paramName, paramName))
			}
			return nil
		}
	case oNumber:
		{
			value := paramValue[0]
			//			if lVar, err = cur.NewVar(&value); err != nil {

			//				return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
			//			}
			if lVar, err = cur.NewVariable(1, oracle.FloatVarType, 0); err != nil {

				return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
			}
			lVar.SetValue(0, value)

			params[paramName] = lVar

			// stmExecDeclarePart
			stmShowDeclarePart.WriteString(fmt.Sprintf("  l_%s %s;\n", paramName, paramTypeName))
			//stmExecSetPart,
			stmShowSetPart.WriteString(fmt.Sprintf("  l_%s := %s;\n", paramName, value))
			// Вызов процедуры - Формирование строки с параметрами для вызова процедуры
			if stmExecProcParams.Len() != 0 {
				stmExecProcParams.WriteString(", ")
			}
			stmExecProcParams.WriteString(fmt.Sprintf("%s => :%s", paramName, paramName))

			// Отображение вызова процедуры - Формирование строки с параметрами для вызова процедуры
			if stmShowProcParams.Len() != 0 {
				stmShowProcParams.WriteString(", ")
			}
			stmShowProcParams.WriteString(fmt.Sprintf("%s => l_%s", paramName, paramName))

			// Добавление вызова сохранения параметра
			if paramStoreProc != "" {
				if lVar, err = cur.NewVar(&value); err != nil {
					return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
				}
				params[paramName+"_s"] = lVar
				stmExecStoreInContext.WriteString(fmt.Sprintf("  %s('%s', :%s_s);\n", paramStoreProc, paramName, paramName))
				stmShowStoreInContext.WriteString(fmt.Sprintf("  %s('%s', l_%s);\n", paramStoreProc, paramName, paramName))
			}
			return nil
		}
	case oInteger:
		{
			value := paramValue[0]
			if lVar, err = cur.NewVar(&value); err != nil {

				return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
			}
			params[paramName] = lVar

			// stmExecDeclarePart
			stmShowDeclarePart.WriteString(fmt.Sprintf("  l_%s %s;\n", paramName, paramTypeName))
			//stmExecSetPart,
			stmShowSetPart.WriteString(fmt.Sprintf("  l_%s := %s;\n", paramName, value))
			// Вызов процедуры - Формирование строки с параметрами для вызова процедуры
			if stmExecProcParams.Len() != 0 {
				stmExecProcParams.WriteString(", ")
			}
			stmExecProcParams.WriteString(fmt.Sprintf("%s => :%s", paramName, paramName))

			// Отображение вызова процедуры - Формирование строки с параметрами для вызова процедуры
			if stmShowProcParams.Len() != 0 {
				stmShowProcParams.WriteString(", ")
			}
			stmShowProcParams.WriteString(fmt.Sprintf("%s => l_%s", paramName, paramName))

			// Добавление вызова сохранения параметра
			if paramStoreProc != "" {
				if lVar, err = cur.NewVar(&value); err != nil {
					return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
				}
				params[paramName+"_s"] = lVar
				stmExecStoreInContext.WriteString(fmt.Sprintf("  %s('%s', :%s_s);\n", paramStoreProc, paramName, paramName))
				stmShowStoreInContext.WriteString(fmt.Sprintf("  %s('%s', l_%s);\n", paramStoreProc, paramName, paramName))
			}
			return nil
		}

	case oDate:
		{
			value := paramValue[0]
			if lVar, err = cur.NewVar(&value); err != nil {
				return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
			}
			params[paramName] = lVar

			// stmExecDeclarePart
			stmShowDeclarePart.WriteString(fmt.Sprintf("  l_%s %s;\n", paramName, paramTypeName))
			//stmExecSetPart,
			stmShowSetPart.WriteString(fmt.Sprintf("  l_%s := to_date('%s');\n", paramName, value))
			// Вызов процедуры - Формирование строки с параметрами для вызова процедуры
			if stmExecProcParams.Len() != 0 {
				stmExecProcParams.WriteString(", ")
			}
			stmExecProcParams.WriteString(fmt.Sprintf("%s => :%s", paramName, paramName))
			// Отображение вызова процедуры - Формирование строки с параметрами для вызова процедуры
			if stmShowProcParams.Len() != 0 {
				stmShowProcParams.WriteString(", ")
			}
			stmShowProcParams.WriteString(fmt.Sprintf("%s => l_%s", paramName, paramName))

			// Добавление вызова сохранения параметра
			if paramStoreProc != "" {
				if lVar, err = cur.NewVar(&value); err != nil {
					return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
				}
				params[paramName+"_s"] = lVar
				stmExecStoreInContext.WriteString(fmt.Sprintf("  %s('%s', :%s_s);\n", paramStoreProc, paramName, paramName))
				stmShowStoreInContext.WriteString(fmt.Sprintf("  %s('%s', l_%s);\n", paramStoreProc, paramName, paramName))
			}
			return nil
		}
	case oBoolean:
		{
			value := paramValue[0]
			if lVar, err = cur.NewVar(&value); err != nil {
				return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
			}
			params[paramName] = lVar

			// stmExecDeclarePart
			stmShowDeclarePart.WriteString(fmt.Sprintf("  l_%s %s;\n", paramName, paramTypeName))
			//stmExecSetPart,
			stmShowSetPart.WriteString(fmt.Sprintf("  l_%s := %s;\n", paramName, value))
			// Вызов процедуры - Формирование строки с параметрами для вызова процедуры
			if stmExecProcParams.Len() != 0 {
				stmExecProcParams.WriteString(", ")
			}
			stmExecProcParams.WriteString(fmt.Sprintf("%s => :%s", paramName, paramName))
			// Отображение вызова процедуры - Формирование строки с параметрами для вызова процедуры
			if stmShowProcParams.Len() != 0 {
				stmShowProcParams.WriteString(", ")
			}
			stmShowProcParams.WriteString(fmt.Sprintf("%s => l_%s", paramName, paramName))

			// Добавление вызова сохранения параметра
			if paramStoreProc != "" {
				stmExecStoreInContext.WriteString(fmt.Sprintf("  %s('%s', '%s');\n", paramStoreProc, paramName, value))
				stmShowStoreInContext.WriteString(fmt.Sprintf("  %s('%s', '%s');\n", paramStoreProc, paramName, value))
			}
			return nil
		}
	case oStringTab, oNumberTab, oIntegerTab, oDateTab, oBooleanTab:
		{
			value := make([]interface{}, len(paramValue))
			valueMaxLen := 0
			for i, val := range paramValue {
				value[i] = val
				if len(val) > valueMaxLen {
					valueMaxLen = len(val)
				}
			}
			switch paramType {
			case oStringTab:
				{
					if lVar, err = cur.NewArrayVar(oracle.StringVarType, value, uint(valueMaxLen)); err != nil {
						return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
					}
					params[paramName] = lVar

					// stmExecDeclarePart
					stmShowDeclarePart.WriteString(fmt.Sprintf("  l_%s %s;\n", paramName, paramTypeName))
					//stmExecSetPart,
					for i := range paramValue {
						stmShowSetPart.WriteString(fmt.Sprintf("  l_%s(%d) := '%s';\n", paramName, i+1, strings.Replace(paramValue[i], "'", "''", -1)))
					}
					// Вызов процедуры - Формирование строки с параметрами для вызова процедуры
					if stmExecProcParams.Len() != 0 {
						stmExecProcParams.WriteString(", ")
					}
					stmExecProcParams.WriteString(fmt.Sprintf("%s => :%s", paramName, paramName))
					// Отображение вызова процедуры - Формирование строки с параметрами для вызова процедуры
					if stmShowProcParams.Len() != 0 {
						stmShowProcParams.WriteString(", ")
					}
					stmShowProcParams.WriteString(fmt.Sprintf("%s => l_%s", paramName, paramName))

					// Добавление вызова сохранения параметра
					if paramStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.StringVarType, value, uint(valueMaxLen)); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar

						for i := range paramValue {
							stmExecStoreInContext.WriteString(fmt.Sprintf("  %s('%s', :%s_s(%d));\n", paramStoreProc, paramName, paramName, i+1))
							stmShowStoreInContext.WriteString(fmt.Sprintf("  %s('%s', l_%s(%d));\n", paramStoreProc, paramName, paramName, i+1))
						}
					}

				}
			case oNumberTab:
				{
					if lVar, err = cur.NewArrayVar(oracle.FloatVarType, value, 0); err != nil {
						return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
					}
					params[paramName] = lVar
					// stmExecDeclarePart
					stmShowDeclarePart.WriteString(fmt.Sprintf("  l_%s %s;\n", paramName, paramTypeName))
					//stmExecSetPart,
					for i := range paramValue {
						stmShowSetPart.WriteString(fmt.Sprintf("  l_%s(%d) := %s;\n", paramName, i+1, paramValue[i]))
					}
					// Вызов процедуры - Формирование строки с параметрами для вызова процедуры
					if stmExecProcParams.Len() != 0 {
						stmExecProcParams.WriteString(", ")
					}
					stmExecProcParams.WriteString(fmt.Sprintf("%s => :%s", paramName, paramName))
					// Отображение вызова процедуры - Формирование строки с параметрами для вызова процедуры
					if stmShowProcParams.Len() != 0 {
						stmShowProcParams.WriteString(", ")
					}
					stmShowProcParams.WriteString(fmt.Sprintf("%s => l_%s", paramName, paramName))

					// Добавление вызова сохранения параметра
					if paramStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.FloatVarType, value, uint(valueMaxLen)); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar

						for i := range paramValue {
							stmExecStoreInContext.WriteString(fmt.Sprintf("  %s('%s', :%s_s(%d));\n", paramStoreProc, paramName, paramName, i+1))
							stmShowStoreInContext.WriteString(fmt.Sprintf("  %s('%s', l_%s(%d));\n", paramStoreProc, paramName, paramName, i+1))
						}
					}
				}
			case oIntegerTab:
				{
					if lVar, err = cur.NewArrayVar(oracle.Int32VarType, value, 0); err != nil {
						return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
					}
					params[paramName] = lVar
					// stmExecDeclarePart
					stmShowDeclarePart.WriteString(fmt.Sprintf("  l_%s %s;\n", paramName, paramTypeName))
					//stmExecSetPart,
					for i := range paramValue {
						stmShowSetPart.WriteString(fmt.Sprintf("  l_%s(%d) := %s;\n", paramName, i+1, paramValue[i]))
					}
					// Вызов процедуры - Формирование строки с параметрами для вызова процедуры
					if stmExecProcParams.Len() != 0 {
						stmExecProcParams.WriteString(", ")
					}
					stmExecProcParams.WriteString(fmt.Sprintf("%s => :%s", paramName, paramName))
					// Отображение вызова процедуры - Формирование строки с параметрами для вызова процедуры
					if stmShowProcParams.Len() != 0 {
						stmShowProcParams.WriteString(", ")
					}
					stmShowProcParams.WriteString(fmt.Sprintf("%s => l_%s", paramName, paramName))
					// Добавление вызова сохранения параметра
					if paramStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.Int32VarType, (value), 0); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar
						for i := range paramValue {
							stmExecStoreInContext.WriteString(fmt.Sprintf("  %s('%s', :%s_s(%d));\n", paramStoreProc, paramName, paramName, i+1))
							stmShowStoreInContext.WriteString(fmt.Sprintf("  %s('%s', l_%s(%d));\n", paramStoreProc, paramName, paramName, i+1))
						}
					}
				}
			case oDateTab:
				{
					valueTime := make([]interface{}, len(paramValue))
					for i, val := range paramValue {
						valueTime[i], _ = time.Parse(time.RFC3339, val)

					}
					if lVar, err = cur.NewArrayVar(oracle.DateTimeVarType, valueTime, 0); err != nil {
						return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
					}

					params[paramName] = lVar
					// stmExecDeclarePart
					stmShowDeclarePart.WriteString(fmt.Sprintf("  l_%s %s;\n", paramName, paramTypeName))
					//stmExecSetPart,
					for i := range paramValue {
						stmShowSetPart.WriteString(fmt.Sprintf("  l_%s(%d) := to_date('%s');\n", paramName, i+1, paramValue[i]))
					}
					// Вызов процедуры - Формирование строки с параметрами для вызова процедуры
					if stmExecProcParams.Len() != 0 {
						stmExecProcParams.WriteString(", ")
					}
					stmExecProcParams.WriteString(fmt.Sprintf("%s => :%s", paramName, paramName))
					// Отображение вызова процедуры - Формирование строки с параметрами для вызова процедуры
					if stmShowProcParams.Len() != 0 {
						stmShowProcParams.WriteString(", ")
					}
					stmShowProcParams.WriteString(fmt.Sprintf("%s => l_%s", paramName, paramName))
					// Добавление вызова сохранения параметра
					if paramStoreProc != "" {
						if lVar, err = cur.NewArrayVar(oracle.DateTimeVarType, (value), 0); err != nil {
							return errgo.Newf("error creating variable for %s(%T): %s", paramName, value, err)
						}
						params[paramName+"_s"] = lVar
						for i := range paramValue {
							stmExecStoreInContext.WriteString(fmt.Sprintf("  %s('%s', :%s_s(%d));\n", paramStoreProc, paramName, paramName, i+1))
							stmShowStoreInContext.WriteString(fmt.Sprintf("  %s('%s', l_%s(%d));\n", paramStoreProc, paramName, paramName, i+1))
						}
					}
				}
			default:
				{
					return errgo.Newf("error creating variable for %s(%T): Invalid subtype %v", paramName, value, paramType)
				}
			}
			return nil
		}
	default:
		{

		}
	}
	return nil
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
