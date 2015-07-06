// oraparamcache
package otasker

import (
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

type oracleDescribedProc struct {
	timestamp   time.Time
	packageName string
	params      map[string]oracleDescribedProcParam
}

type oracleDescriber struct {
	sync.Mutex
	procs map[string]oracleDescribedProc
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
        ,:overload
        ,:position
        ,:level
        ,:argument_name
        ,ldatatype
        ,:default_value
        ,:in_out
        ,:length
        ,:precision
        ,:scale
        ,:radix
        ,:spare
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
		err error

		overload       = make([]interface{}, 4000)
		position       = make([]interface{}, 4000)
		level          = make([]interface{}, 4000)
		argumentName   = make([]interface{}, 4000)
		datatype       = make([]interface{}, 4000)
		defaultValue   = make([]interface{}, 4000)
		inOut          = make([]interface{}, 4000)
		length         = make([]interface{}, 4000)
		precision      = make([]interface{}, 4000)
		scale          = make([]interface{}, 4000)
		radix          = make([]interface{}, 4000)
		spare          = make([]interface{}, 4000)
		packageName    string
		lastChangeTime time.Time
		updated        int32
		arrayLen       int32

		procNameVar       *oracle.Variable
		overloadVar       *oracle.Variable
		positionVar       *oracle.Variable
		levelVar          *oracle.Variable
		argumentNameVar   *oracle.Variable
		datatypeVar       *oracle.Variable
		defaultValueVar   *oracle.Variable
		inOutVar          *oracle.Variable
		lengthVar         *oracle.Variable
		precisionVar      *oracle.Variable
		scaleVar          *oracle.Variable
		radixVar          *oracle.Variable
		spareVar          *oracle.Variable
		packageNameVar    *oracle.Variable
		lastChangeTimeVar *oracle.Variable
		updatedVar        *oracle.Variable
		arrayLenVar       *oracle.Variable
	)

	defer func() {
		procNameVar.Free()
		overloadVar.Free()
		positionVar.Free()
		levelVar.Free()
		argumentNameVar.Free()
		datatypeVar.Free()
		defaultValueVar.Free()
		inOutVar.Free()
		lengthVar.Free()
		precisionVar.Free()
		scaleVar.Free()
		radixVar.Free()
		spareVar.Free()
		packageNameVar.Free()
		lastChangeTimeVar.Free()
		updatedVar.Free()
		arrayLenVar.Free()
	}()

	dpp, ok := d.procs[procName]
	if !ok {
		lastChangeTime = time.Time{}
	} else {
		lastChangeTime = dpp.timestamp
	}

	if procNameVar, err = cur.NewVar(&procName); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", procName, procName, err)
	}

	if overloadVar, err = cur.NewArrayVar(oracle.Int32VarType, overload, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", overload, overload, err)
	}
	if positionVar, err = cur.NewArrayVar(oracle.Int32VarType, position, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", position, position, err)
	}
	if levelVar, err = cur.NewArrayVar(oracle.Int32VarType, level, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", level, level, err)
	}
	if argumentNameVar, err = cur.NewArrayVar(oracle.StringVarType, argumentName, 30); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", argumentName, argumentName, err)
	}
	if datatypeVar, err = cur.NewArrayVar(oracle.Int32VarType, datatype, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", datatype, datatype, err)
	}
	if defaultValueVar, err = cur.NewArrayVar(oracle.Int32VarType, defaultValue, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", defaultValue, defaultValue, err)
	}
	if inOutVar, err = cur.NewArrayVar(oracle.Int32VarType, inOut, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", inOut, inOut, err)
	}
	if lengthVar, err = cur.NewArrayVar(oracle.Int32VarType, length, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", length, length, err)
	}
	if precisionVar, err = cur.NewArrayVar(oracle.Int32VarType, precision, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", precision, precision, err)
	}
	if scaleVar, err = cur.NewArrayVar(oracle.Int32VarType, scale, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", scale, scale, err)
	}
	if radixVar, err = cur.NewArrayVar(oracle.Int32VarType, radix, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", radix, radix, err)
	}
	if spareVar, err = cur.NewArrayVar(oracle.Int32VarType, spare, 0); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", spare, spare, err)
	}

	if packageNameVar, err = cur.NewVar(&packageName); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", packageName, packageName, err)
	}

	if lastChangeTimeVar, err = cur.NewVar(&lastChangeTime); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", lastChangeTime, lastChangeTime, err)
	}
	if updatedVar, err = cur.NewVar(&updated); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", updated, updated, err)
	}
	if arrayLenVar, err = cur.NewVar(&arrayLen); err != nil {
		return nil, errgo.Newf("error creating variable for %s(%T): %s", arrayLen, arrayLen, err)
	}

	if err := cur.Execute(stm, nil, map[string]interface{}{"proc_name": procNameVar,
		"overload":      overloadVar,
		"position":      positionVar,
		"level":         levelVar,
		"argument_name": argumentNameVar,
		"datatype":      datatypeVar,
		"default_value": defaultValueVar,
		"in_out":        inOutVar,
		"length":        lengthVar,
		"precision":     precisionVar,
		"scale":         scaleVar,
		"radix":         radixVar,
		"spare":         spareVar,
		"package_name":  packageNameVar,
		"last_ddl_time": lastChangeTimeVar,
		"updated":       updatedVar,
		"len_":          arrayLenVar,
	}); err != nil {
		return nil, errgo.Newf("Невозможно получить описание для \"%s\"\nОшибка: %s", procName, err.Error())
	}

	if updated == 1 {
		if !ok {
			dpp = oracleDescribedProc{}
			d.procs[procName] = dpp
		}
		dpp.packageName = packageName
		dpp.timestamp = lastChangeTime
		dpp.params = make(map[string]oracleDescribedProcParam)
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
					for j := i + 1; j < len(datatype); j++ {
						if level[i] == level[j] {
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

					dpp.params[paramName] = oracleDescribedProcParam{dataType: paramDataType,
						dataSubType: paramSubDataType,
						level:       paramLevel,
						length:      paramLength}
					i = i + 1
				}
			default:
				{
					dpp.params[paramName] = oracleDescribedProcParam{dataType: paramDataType,
						dataSubType: paramSubDataType,
						level:       paramLevel,
						length:      paramLength}
				}
			}

		}
		return &dpp, nil

	}
	return &dpp, nil
}

func (d *oracleDescriber) Clear() {
	d.Lock()
	defer d.Unlock()
	for k := range d.procs {
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
	return &oracleDescriber{procs: make(map[string]oracleDescribedProc)}
}
