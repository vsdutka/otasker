// operationlogger
package otasker

import (
	"encoding/gob"
	"fmt"
	"gopkg.in/goracle.v1/oracle"
	"os"
	"path/filepath"
	//"reflect"
	"sync"
	"sync/atomic"
	"time"
)

const (
	opLogin = iota
	opLogout
	opExec
)

type oracleOperationParam struct {
	Type  oracle.VariableType
	Value []interface{}
}
type OracleOperation struct {
	StreamID    string
	OperationID uint64
	Type        uint
	UserName    string
	UserPass    string
	ConnStr     string
	Stm         string
	Params      map[string]oracleOperationParam
	ClientBg    time.Time
	ClientFn    time.Time
	ServerBg    time.Time
	ServerFn    time.Time
	ServerFnScn string
	Success     bool
}

//func (op *OracleOperation) GobEncode() ([]byte, error) {
//	return nil, nil
//}

//func (op *OracleOperation) GobDecode([]byte) error {
//	return nil
//}

type oracleOperationLogger struct {
	channels map[string]chan OracleOperation
	sync.Mutex
}

var logger = oracleOperationLogger{channels: make(map[string]chan OracleOperation)}

var opCounter uint64

func init() {
	//gob.Register(map[string]interface{}(nil))
	gob.Register([]interface{}(nil))
	//gob.Register(reflect.Value{})
	gob.Register(time.Time{})
}

func (l *oracleOperationLogger) getLoggerChannel(loggerName string) chan OracleOperation {
	l.Lock()
	defer l.Unlock()
	res, ok := l.channels[loggerName]
	if !ok {
		res = make(chan OracleOperation, 10000)
		l.channels[loggerName] = res
		go func(loggerName string, opChan chan OracleOperation) {
			dir, _ := filepath.Split(loggerName)
			os.MkdirAll(dir, os.ModeDir)

			f, err := os.OpenFile(loggerName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			enc := gob.NewEncoder(f)

			for {
				select {
				case op := <-opChan:
					{

						err := enc.Encode(&op)
						if err != nil {
							fmt.Println(err)
						}
					}
				}
			}
		}(loggerName, res)
	}
	return res
}

func loginOp(loggerName, streamID, userName, userPass, connStr string, clientBg, clientFn time.Time, success bool) {
	if loggerName == "" {
		return
	}
	c := logger.getLoggerChannel(loggerName)

	c <- OracleOperation{StreamID: streamID,
		OperationID: atomic.AddUint64(&opCounter, 1),
		Type:        opLogin,
		UserName:    userName,
		UserPass:    userPass,
		ConnStr:     connStr,
		Stm:         "",
		Params:      make(map[string]oracleOperationParam, 0),
		ClientBg:    clientBg,
		ClientFn:    clientFn,
		ServerBg:    clientBg,
		ServerFn:    clientFn,
		ServerFnScn: "",
		Success:     success,
	}
}

func logoutOp(loggerName, streamID, userName, userPass, connStr string, clientBg, clientFn time.Time, success bool) {
	if loggerName == "" {
		return
	}
	c := logger.getLoggerChannel(loggerName)

	c <- OracleOperation{StreamID: streamID,
		OperationID: atomic.AddUint64(&opCounter, 1),
		Type:        opLogout,
		UserName:    userName,
		UserPass:    userPass,
		ConnStr:     connStr,
		Stm:         "",
		Params:      make(map[string]oracleOperationParam, 0),
		ClientBg:    clientBg,
		ClientFn:    clientFn,
		ServerBg:    clientBg,
		ServerFn:    clientFn,
		ServerFnScn: "",
		Success:     success,
	}
}

func execOp(loggerName, streamID, userName, userPass, connStr, stm string, params map[string]interface{}, clientBg, clientFn time.Time, success bool) {
	var (
		serverBg    time.Time
		serverFn    time.Time
		serverFnScn string
	)
	if loggerName == "" {
		return
	}

	lParams := make(map[string]oracleOperationParam)
	for key, val := range params {
		oraVar := val.(*oracle.Variable)
		oraVarType, _, _, _ := oracle.VarTypeByValue(oraVar)
		//fmt.Println(key, " ", oraVarType, " size = ", size, " numElements = ", numElements, " type = ", reflect.TypeOf(vt).Name())

		lParam := oracleOperationParam{Type: *oraVarType, Value: make([]interface{}, 0)}
		for i := uint(0); i < oraVar.ArrayLength(); i++ {
			paramValItem, err := oraVar.GetValue(i)
			if err != nil {
				panic(err)
			}
			lParam.Value = append(lParam.Value, paramValItem)
		}
		//fmt.Println(key, " ", lParam, " ", oraVarType, " ", oraVar)
		//		if oraVar.ArrayLength() > 0 {
		//			var paramVal []interface{}
		//			for i := uint(1); i < oraVar.ArrayLength()+1; i++ {
		//				paramValItem, err := oraVar.GetValue(i - 1)
		//				if err != nil {
		//					panic(err)
		//				}

		//				if paramValItem != nil {
		//					fmt.Println(key, " ", reflect.TypeOf(oraVar).Kind(), " ", reflect.TypeOf(paramValItem).Kind())
		//					paramVal = append(paramVal, reflect.ValueOf(paramValItem))
		//				} else {
		//					paramVal = append(paramVal, reflect.ValueOf(paramValItem))
		//					fmt.Println(key, " ", reflect.TypeOf(oraVar).Kind(), " ", reflect.ValueOf(paramValItem))
		//				}
		//			}
		//			lParams[key] = lParam
		//		} else {
		//			paramVal, err := oraVar.GetValue(0)
		//			if err == nil {
		//				if reflect.ValueOf(paramVal).Kind() == reflect.Ptr {
		//					// Lob
		//					var buf []byte
		//					ext, ok := paramVal.(*oracle.ExternalLobVar)
		//					if !ok {
		//						panic(fmt.Sprintf("%s is not *ExternalLobVar, but %T", key, paramVal))
		//					}
		//					if ext != nil {
		//						size, _ := ext.Size(false)
		//						if size > 0 {
		//							buf, err = ext.ReadAll()
		//							if err != nil {
		//								panic(err)
		//							}

		//						}
		//					}
		//					lParams[key] = buf
		//				} else {
		//					if paramVal != nil {
		//						fmt.Println(key, " ", reflect.TypeOf(oraVar).Kind(), " ", reflect.TypeOf(paramVal).Kind())
		//						lParams[key] = reflect.ValueOf(paramVal)
		//						switch key {
		//						case "server_bg":
		//							serverBg, _ = time.Parse("2006-01-02 15:04:05.999999999 -07:00", paramVal.(string))
		//						case "server_fn":
		//							serverFn, _ = time.Parse("2006-01-02 15:04:05.999999999 -07:00", paramVal.(string))
		//						case "server_fn_scn":
		//							serverFnScn, _ = paramVal.(string)
		//						}
		//					} else {
		//						lParams[key] = lParam
		//						fmt.Println(key, " ", reflect.TypeOf(oraVar).Kind(), " ", reflect.ValueOf(paramVal))
		//					}
		//				}
		//			} else {
		//				lParams[key] = nil
		//			}
		//		}
		lParams[key] = lParam

	}

	c := logger.getLoggerChannel(loggerName)

	c <- OracleOperation{StreamID: streamID,
		OperationID: atomic.AddUint64(&opCounter, 1),
		Type:        opExec,
		UserName:    userName,
		UserPass:    userPass,
		ConnStr:     connStr,
		Stm:         stm,
		Params:      lParams,
		ClientBg:    clientBg,
		ClientFn:    clientFn,
		ServerBg:    serverBg,
		ServerFn:    serverFn,
		ServerFnScn: serverFnScn,
		Success:     success,
	}
}
