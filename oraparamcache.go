// oraparamcache
package otasker

import (
	//"fmt"
	"github.com/vsdutka/oracleex"
	"gopkg.in/errgo.v1"
	"gopkg.in/goracle.v1/oracle"
	"strings"
	"sync"
	"time"
)

const (
	// External Oracle Datatypes
	otInteger     = 3
	otFloat       = 4
	otString      = 5
	otLong        = 8
	otDate        = 12
	otLongRaw     = 24
	otBoolean     = 252 // Does not work as bind variable!
	otCLOB        = 112
	otNCLOB       = -112
	otBLOB        = 113
	otBFile       = 114
	otCursor      = 116
	otObject      = 108
	otReference   = 110
	otDBChar      = 96
	otChar        = 97
	otPLSQLString = 10
	otSubst       = 1 // Substitution variable, will be replaced in the SQL text
	otTimestamp   = 187

	// Internal Oracle Datatypes
	otVarchar2 = 1
	otNumber   = 2
	otVarchar  = 9
	otRowID    = 11
	otRaw      = 23
	otMLSLabel = 106
	// Oracle8
	otRowidDesc = 104
	// Oracle9
	otTimestampTZ  = 188
	otTimestampLTZ = 232
	otIntervalYM   = 189
	otIntervalDS   = 190
	// Oracle10
	otBinaryFloat  = 100
	otBinaryDouble = 101

	otNestedTableTypeOracle8 = 122
	otVariableArrayOracle8   = 123
	otRecordType             = 250
	otPLSQLIndexByTableType  = 251
)

// OracleDescribedProc - интерфейс для работы с кешем параметров по конкретной процедуре
type OracleDescribedProc interface {
	PackageName() string
	ParamDataType(paramName string) int32
	ParamDataSubType(paramName string) int32
	ParamLevel(paramName string) int32
	ParamLength(paramName string) int32
}

// OracleDescriber - интерфейс для работы с кешем параметров процедур в сессии
type OracleDescriber interface {
	Describe(r *oracleTasker, conn *oracleex.Connection, procName string) (OracleDescribedProc, error)
	Clear()
}

type oracleDescribedProcParam struct {
	dataType    int32
	dataSubType int32
	level       int32
	length      int32
}

var dppFree = sync.Pool{
	New: func() interface{} { return new(oracleDescribedProcParam) },
}

type oracleDescribedProc struct {
	timestamp   time.Time
	packageName string
	params      map[string]*oracleDescribedProcParam
}

type oracleDescriber struct {
	sync.Mutex
	procs map[string]*oracleDescribedProc
}

func (d *oracleDescriber) Describe(r *oracleTasker, conn *oracleex.Connection, procName string) (OracleDescribedProc, error) {
	const (
		stm = `declare
  lstatus varchar2(40);
  lschema VARCHAR2(40);
  lpart1 VARCHAR2(40);
  lpart2 VARCHAR2(40);
  ldblink VARCHAR2(40);
  lpart1_type NUMBER;
  lobject_number NUMBER;
  lobject_type VARCHAR2(40);
  llast_ddl_time date;
  ldatatype sys.dbms_describe.number_table;
  llen pls_integer;
  overload sys.dbms_describe.number_table;
  position sys.dbms_describe.number_table;
  default_value sys.dbms_describe.number_table;
  in_out sys.dbms_describe.number_table;
  precision sys.dbms_describe.number_table;
  scale sys.dbms_describe.number_table;
  radix sys.dbms_describe.number_table;
  spare sys.dbms_describe.number_table;
  ex1 exception;
  pragma exception_init(ex1, -06564);
begin
  DBMS_UTILITY.NAME_RESOLVE(:proc_name,1,lschema,lpart1,lpart2,ldblink,lpart1_type,lobject_number);

  if lpart1_type = 9 then
    :package_name := lschema || '.' || lpart1;
  else
    :package_name := null;
  end if;

  select status, object_type, last_ddl_time
  into lstatus, lobject_type, llast_ddl_time
  from all_objects
  where all_objects.object_id=lobject_number;
  if lstatus='INVALID' then
    dbms_ddl.alter_compile(lobject_type,lschema,nvl(lpart1,lpart2));
  end if;
  if llast_ddl_time <= :last_ddl_time then
    :updated := 0;
  else
    :updated := 1;
    :last_ddl_time := llast_ddl_time;

    dbms_describe.describe_procedure
      (
        :proc_name
        ,null
        ,null
        ,overload
        ,position
        ,:level
        ,:argument_name
        ,ldatatype
        ,default_value
        ,in_out
        ,:length
        ,precision
        ,scale
        ,radix
        ,spare
      );
	:datatype := ldatatype;
	llen := ldatatype.count();
	if ldatatype.count() = 1 then
	  if ldatatype(1) = 0 then
	    llen := 0;
	  end if;
	end if;
	:len_ := llen;
  end if;
  commit;
exception
  when others then
    rollback;
    if sqlcode in (-20000, -20001, -20002, -20003, -20004) then
      raise ex1;
    else
       raise;
    end if;
end;`
	)
	d.Lock()
	cur := conn.NewCursor()
	defer func() { cur.Close(); d.Unlock() }()

	var (
		err               error
		lastChangeTime    time.Time
		updated           int32
		arrayLen          int32
		procNameVar       *oracle.Variable
		levelVar          *oracle.Variable
		argumentNameVar   *oracle.Variable
		datatypeVar       *oracle.Variable
		lengthVar         *oracle.Variable
		packageNameVar    *oracle.Variable
		lastChangeTimeVar *oracle.Variable
		updatedVar        *oracle.Variable
		arrayLenVar       *oracle.Variable
	)

	dpp, ok := d.procs[procName]
	if !ok {
		lastChangeTime = time.Time{}
	} else {
		lastChangeTime = dpp.timestamp
	}

	if procNameVar, err = cur.NewVar(&procName); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", procName, procName, err)
	}
	defer procNameVar.Free()

	if levelVar, err = cur.NewVariable(4000, oracle.Int32VarType, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", "level", "number", err)
	}
	defer levelVar.Free()

	if argumentNameVar, err = cur.NewVariable(4000, oracle.StringVarType, 30); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", "argumentName", "string", err)
	}
	defer argumentNameVar.Free()

	if datatypeVar, err = cur.NewVariable(4000, oracle.Int32VarType, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", "datatype", "number", err)
	}
	defer datatypeVar.Free()

	if lengthVar, err = cur.NewVariable(4000, oracle.Int32VarType, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", "length", "number", err)
	}
	defer lengthVar.Free()

	if packageNameVar, err = cur.NewVariable(0, oracle.StringVarType, 240); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", "packageName", "string", err)
	}
	defer packageNameVar.Free()

	if lastChangeTimeVar, err = cur.NewVar(&lastChangeTime); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", lastChangeTime, lastChangeTime, err)
	}
	defer lastChangeTimeVar.Free()

	if updatedVar, err = cur.NewVar(&updated); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", updated, updated, err)
	}
	defer updatedVar.Free()

	if arrayLenVar, err = cur.NewVar(&arrayLen); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", arrayLen, arrayLen, err)
	}
	defer arrayLenVar.Free()

	if err := cur.Execute(stm, nil, map[string]interface{}{"proc_name": procNameVar,

		"level":         levelVar,
		"argument_name": argumentNameVar,
		"datatype":      datatypeVar,
		"length":        lengthVar,
		"package_name":  packageNameVar,
		"last_ddl_time": lastChangeTimeVar,
		"updated":       updatedVar,
		"len_":          arrayLenVar,
	}); err != nil {
		return nil, errgo.Newf("Невозможно получить описание для \"%s\"\nОшибка: %s", procName, err.Error())
	}

	if updated == 1 {
		if !ok {
			dpp = &oracleDescribedProc{}
			d.procs[procName] = dpp
		}
		if packageName, err := packageNameVar.GetValue(0); err != nil {
			return nil, err
		} else {
			if packageName != nil {
				dpp.packageName = packageName.(string)
			} else {
				dpp.packageName = ""
			}
		}
		dpp.timestamp = lastChangeTime
		dpp.params = make(map[string]*oracleDescribedProcParam)
		for i := 0; i < int(arrayLen); i++ {
			var (
				paramName        string
				paramDataType    int32
				paramSubDataType int32
				paramLevel       int32
				paramLength      int32
			)

			intf, err := argumentNameVar.GetValue(uint(i))
			if err != nil {
				return nil, err
			}
			paramName = intf.(string)

			intf, err = datatypeVar.GetValue(uint(i))

			if err != nil {
				return nil, err
			}
			paramDataType = intf.(int32)
			intf, err = levelVar.GetValue(uint(i))
			if err != nil {
				return nil, err
			}
			paramLevel = intf.(int32)
			intf, err = lengthVar.GetValue(uint(i))
			if err != nil {
				return nil, err
			}
			paramLength = intf.(int32)

			paramSubDataType = int32(0)

			switch paramDataType {
			case otNestedTableTypeOracle8, otVariableArrayOracle8, otRecordType:
				{
					// Пропускаем информацию о вложенных данных
					for j := i + 1; j < int(arrayLen); j++ {
						intf, err = levelVar.GetValue(uint(j))
						if err != nil {
							return nil, err
						}
						subParamLevel := intf.(int32)
						if paramLevel == subParamLevel {
							i = j - 1
							break
						}
					}
				}
			case otPLSQLIndexByTableType:
				{
					intf, err = datatypeVar.GetValue(uint(i + 1))
					if err != nil {
						return nil, err
					}
					paramSubDataType = intf.(int32)

					p := dppFree.Get()
					paramInstance := p.(*oracleDescribedProcParam)
					paramInstance.dataType = paramDataType
					paramInstance.dataSubType = paramSubDataType
					paramInstance.level = paramLevel
					paramInstance.length = paramLength

					dpp.params[paramName] = paramInstance

					//					dpp.params[paramName] = oracleDescribedProcParam{dataType: paramDataType,
					//						dataSubType: paramSubDataType,
					//						level:       paramLevel,
					//						length:      paramLength}
					i = i + 1
				}
			default:
				{
					p := dppFree.Get()
					paramInstance := p.(*oracleDescribedProcParam)
					paramInstance.dataType = paramDataType
					paramInstance.dataSubType = paramSubDataType
					paramInstance.level = paramLevel
					paramInstance.length = paramLength

					dpp.params[paramName] = paramInstance
					//					dpp.params[paramName] = oracleDescribedProcParam{dataType: paramDataType,
					//						dataSubType: paramSubDataType,
					//						level:       paramLevel,
					//						length:      paramLength}
				}
			}

		}
		return dpp, nil

	}
	return dpp, nil
}

func (d *oracleDescriber) Clear() {
	d.Lock()
	defer d.Unlock()
	for k := range d.procs {
		for l := range d.procs[k].params {
			p := d.procs[k].params[l]
			dppFree.Put(p)
			delete(d.procs[k].params, l)
		}
		delete(d.procs, k)
	}
}

func (dp *oracleDescribedProc) PackageName() string {
	return dp.packageName

}

func (dp *oracleDescribedProc) ParamDataType(paramName string) int32 {
	dpp, ok := dp.params[strings.ToUpper(paramName)]
	if !ok {
		return -1
	}
	return dpp.dataType

}

func (dp *oracleDescribedProc) ParamDataSubType(paramName string) int32 {
	dpp, ok := dp.params[strings.ToUpper(paramName)]
	if !ok {
		return -1
	}
	return dpp.dataSubType

}
func (dp *oracleDescribedProc) ParamLevel(paramName string) int32 {
	dpp, ok := dp.params[strings.ToUpper(paramName)]
	if !ok {
		return -1
	}
	return dpp.level

}
func (dp *oracleDescribedProc) ParamLength(paramName string) int32 {
	dpp, ok := dp.params[strings.ToUpper(paramName)]
	if !ok {
		return -1
	}
	return dpp.length

}

// NewOracleDescriber - создание нового экземпляра объекта для работы с параметрами процедур в кеше
func NewOracleDescriber() OracleDescriber {
	return &oracleDescriber{procs: make(map[string]*oracleDescribedProc)}
}
