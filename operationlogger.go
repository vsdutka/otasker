// operationlogger
package otasker

import (
	"fmt"
	"gopkg.in/goracle.v1/oracle"
	"reflect"
	"sync/atomic"
	"time"
)

const (
	opLogin = iota
	opLogout
	opExec
)

type OracleOperation struct {
	StreamID    string
	OperationID uint64
	Type        uint
	UserName    string
	UserPass    string
	ConnStr     string
	Stm         string
	Params      map[string]interface{}
	ClientBg    time.Time
	ClientFn    time.Time
	ServerBg    time.Time
	ServerFn    time.Time
	ServerFnScn string
	Success     bool
}

var opCounter uint64

func loginOp(f func(op *OracleOperation), streamID, userName, userPass, connStr string, clientBg, clientFn time.Time, success bool) {
	f(&OracleOperation{StreamID: streamID,
		OperationID: atomic.AddUint64(&opCounter, 1),
		Type:        opLogin,
		UserName:    userName,
		UserPass:    userPass,
		ConnStr:     connStr,
		Stm:         "",
		Params:      make(map[string]interface{}, 0),
		ClientBg:    clientBg,
		ClientFn:    clientFn,
		ServerBg:    clientBg,
		ServerFn:    clientFn,
		ServerFnScn: "",
		Success:     success,
	})
}

func logoutOp(f func(op *OracleOperation), streamID, userName, userPass, connStr string, clientBg, clientFn time.Time, success bool) {
	f(&OracleOperation{StreamID: streamID,
		OperationID: atomic.AddUint64(&opCounter, 1),
		Type:        opLogout,
		UserName:    userName,
		UserPass:    userPass,
		ConnStr:     connStr,
		Stm:         "",
		Params:      make(map[string]interface{}, 0),
		ClientBg:    clientBg,
		ClientFn:    clientFn,
		ServerBg:    clientBg,
		ServerFn:    clientFn,
		ServerFnScn: "",
		Success:     success,
	})
}

func execOp(f func(op *OracleOperation), streamID, userName, userPass, connStr, stm string, params map[string]interface{}, clientBg, clientFn time.Time, success bool) {
	var (
		serverBg    time.Time
		serverFn    time.Time
		serverFnScn string
	)

	lParams := make(map[string]interface{})
	for key, val := range params {
		oraVar := val.(*oracle.Variable)
		if oraVar.ArrayLength() > 0 {
			var paramVal []interface{}
			for i := uint(1); i < oraVar.ArrayLength()+1; i++ {
				paramValItem, err := oraVar.GetValue(i - 1)
				if err != nil {
					panic(err)
				}
				paramVal = append(paramVal, reflect.ValueOf(paramValItem))
			}
			lParams[key] = paramVal
		} else {
			paramVal, err := oraVar.GetValue(0)
			if err == nil {
				if reflect.ValueOf(paramVal).Kind() == reflect.Ptr {
					// Lob
					var buf []byte
					ext, ok := paramVal.(*oracle.ExternalLobVar)
					if !ok {
						panic(fmt.Sprintf("%s is not *ExternalLobVar, but %T", key, paramVal))
					}
					if ext != nil {
						size, _ := ext.Size(false)
						if size > 0 {
							buf, err = ext.ReadAll()
							if err != nil {
								panic(err)
							}

						}
					}
					lParams[key] = buf
				} else {
					lParams[key] = reflect.ValueOf(paramVal)
					switch key {
					case "server_bg":
						serverBg, _ = time.Parse("2006-01-02 15:04:05.999999999 -07:00", paramVal.(string))
					case "server_fn":
						serverFn, _ = time.Parse("2006-01-02 15:04:05.999999999 -07:00", paramVal.(string))
					case "server_fn_scn":
						serverFnScn, _ = paramVal.(string)
					}
				}
			} else {
				lParams[key] = nil
			}
		}

	}

	f(&OracleOperation{StreamID: streamID,
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
	})
}
