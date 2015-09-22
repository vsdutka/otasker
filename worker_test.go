// worker_test
package otasker

import (
	"net/url"
	"sync"
	"testing"
	"time"
)

type test struct {
	name         string
	sessionID    string
	taskID       string
	userName     string
	userPass     string
	connStr      string
	procName     string
	procCreate   string
	procDrop     string
	urlValues    url.Values
	files        *Form
	waitTimeout  time.Duration
	idleTimeout  time.Duration
	afterTimeout time.Duration
	resCode      int
	resContent   string
}

func workerRun(t *testing.T, v test) {

	if v.procCreate != "" {
		err := exec(dsn, v.procCreate)
		if err != nil {
			t.Fatalf("%s - Error when run \"%s\": %s", v.name, v.procCreate, err.Error())
		}
	}
	defer func() {
		time.Sleep(v.afterTimeout)
		if v.procDrop != "" {
			err := exec(dsn, v.procDrop)
			if err != nil {
				t.Fatalf("%s - Error when run \"%s\": %s", v.name, v.procDrop, err.Error())
			}
		}
	}()

	res := Run(
		"/asrolf-ti11",
		v.sessionID,
		v.taskID,
		v.userName,
		v.userPass,
		v.connStr,
		"WEX.WS",
		stm_init,
		"",
		"WWV_DOCUMENT",
		cgi,
		v.procName,
		v.urlValues,
		v.files,
		NewOwaClassicProcTasker(),
		v.waitTimeout,
		v.idleTimeout,
		".\\log.log")
	if res.StatusCode != v.resCode {
		t.Log(string(res.Content))
		t.Fatalf("%s: StatusCode - got %v,\nwant %v", v.name, res.StatusCode, v.resCode)
	}
	if string(res.Content) != v.resContent {
		t.Fatalf("%s: Content - got \"%v\",\nwant \"%v\"", v.name, string(res.Content), v.resContent)
	}

}
func TestWorkerRun(t *testing.T) {
	var tests = []test{
		{
			name:      "Вызов простой процедуры",
			sessionID: "sess1",
			taskID:    "TASK1",
			userName:  user,
			userPass:  password,
			connStr:   connstr,
			procName:  "TestWorkerRun",
			procCreate: `
create or replace procedure TestWorkerRun(ap in varchar2) is 
begin
  htp.set_ContentType('text/plain');
  htp.add_CustomHeader('CUSTOM_HEADER: HEADER
CUSTOM_HEADER1: HEADER1
');
  htp.prn(ap);
  hrslt.ADD_FOOTER := false;
  rollback;
end;`,
			procDrop:  `drop procedure TestWorkerRun`,
			urlValues: url.Values{"ap": []string{"1"}},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  10 * time.Second,
			idleTimeout:  2 * time.Second,
			afterTimeout: 3 * time.Second,
			resCode:      200,
			resContent:   "1",
		},
		{
			name:      "Неправильное имя пользователя или пароль",
			sessionID: "sess2",
			taskID:    "TASK2",
			userName:  "user",
			userPass:  password,
			connStr:   connstr,
			procName:  "TestWorkerRun",
			procCreate: `
		create or replace procedure TestWorkerRun(ap in varchar2) is
		begin
		  htp.set_ContentType('text/plain');
		  htp.add_CustomHeader('CUSTOM_HEADER: HEADER
		CUSTOM_HEADER1: HEADER1
		');
		  htp.prn(ap);
		  hrslt.ADD_FOOTER := false;
		  rollback;
		end;`,
			procDrop:  `drop procedure TestWorkerRun`,
			urlValues: url.Values{"ap": []string{"1"}},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  10 * time.Second,
			idleTimeout:  1 * time.Second,
			afterTimeout: 2 * time.Second,
			resCode:      StatusInvalidUsernameOrPassword,
			resContent:   "",
		},
		{
			name:       "Пользователь заблокирован",
			sessionID:  "sess3",
			taskID:     "TASK3",
			userName:   "TEST001",
			userPass:   "1",
			connStr:    connstr,
			procName:   "a.root$.startup",
			procCreate: "begin execute immediate 'create user \"TEST001\" identified by \"1\" account lock'; execute immediate 'grant connect to TEST001'; end;",
			procDrop:   "drop user \"TEST001\"",
			urlValues:  url.Values{},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  10 * time.Second,
			idleTimeout:  1 * time.Second,
			afterTimeout: 2 * time.Second,
			resCode:      StatusAccountIsLocked,
			resContent:   "",
		},
		{
			name:      "Длинный запрос 1 - червяк",
			sessionID: "sess4",
			taskID:    "TASK4",
			userName:  user,
			userPass:  password,
			connStr:   connstr,
			procName:  "TestWorkerRun",
			procCreate: `
create or replace procedure TestWorkerRun(ap in varchar2) is 
begin
  htp.set_ContentType('text/plain');
  htp.add_CustomHeader('CUSTOM_HEADER: HEADER
CUSTOM_HEADER1: HEADER1
');
  htp.prn(ap);
  hrslt.ADD_FOOTER := false;
  dbms_lock.sleep(15);
  rollback;
end;`,
			procDrop:  ``,
			urlValues: url.Values{"ap": []string{"1"}},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  10 * time.Second,
			idleTimeout:  1 * time.Second,
			afterTimeout: 0 * time.Second,
			resCode:      StatusWaitPage,
			resContent:   "",
		},
		{
			name:       "Длинный запрос 1 - результат",
			sessionID:  "sess4",
			taskID:     "TASK4",
			userName:   user,
			userPass:   password,
			connStr:    connstr,
			procName:   "TestWorkerRun",
			procCreate: ``,
			procDrop:   ``,
			urlValues:  url.Values{"ap": []string{"1"}},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  10 * time.Second,
			idleTimeout:  1 * time.Second,
			afterTimeout: 0 * time.Second,
			resCode:      200,
			resContent:   "1",
		},
		{
			name:       "Длинный запрос 2 - червяк",
			sessionID:  "sess5",
			taskID:     "TASK5.1",
			userName:   user,
			userPass:   password,
			connStr:    connstr,
			procName:   "TestWorkerRun",
			procCreate: ``,
			procDrop:   ``,
			urlValues:  url.Values{"ap": []string{"1"}},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  10 * time.Second,
			idleTimeout:  1 * time.Second,
			afterTimeout: 0 * time.Second,
			resCode:      StatusWaitPage,
			resContent:   "",
		},
		{
			name:       "Длинный запрос 2 - прервать запрос",
			sessionID:  "sess5",
			taskID:     "TASK5.2",
			userName:   user,
			userPass:   password,
			connStr:    connstr,
			procName:   "TestWorkerRun",
			procCreate: ``,
			procDrop:   `drop procedure TestWorkerRun`,
			urlValues:  url.Values{"ap": []string{"1"}},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  1 * time.Second,
			idleTimeout:  1 * time.Second,
			afterTimeout: 0 * time.Second,
			resCode:      StatusBreakPage,
			resContent:   "",
		},
		{
			name:       "Длинный запрос 2 - результат",
			sessionID:  "sess5",
			taskID:     "TASK5.1",
			userName:   user,
			userPass:   password,
			connStr:    connstr,
			procName:   "TestWorkerRun",
			procCreate: ``,
			procDrop:   ``,
			urlValues:  url.Values{"ap": []string{"1"}},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  10 * time.Second,
			idleTimeout:  1 * time.Second,
			afterTimeout: 3 * time.Second,
			resCode:      200,
			resContent:   "1",
		},
	}

	for _, v := range tests {
		workerRun(t, v)
	}

	wlock.Lock()
	if len(wlist) > 0 {
		for k, _ := range wlist {
			t.Log(k)
		}
		t.Fatalf("len(wlist) = %d", len(wlist))
	}
	wlock.Unlock()
}

func TestWorkerBreak(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		workerRun(t, test{
			name:      "Длинный запрос 3 - запрос прерван",
			sessionID: "sess6",
			taskID:    "TASK6",
			userName:  user,
			userPass:  password,
			connStr:   connstr,
			procName:  "TestWorkerRun",
			procCreate: `
create or replace procedure TestWorkerRun(ap in varchar2) is 
begin
  htp.set_ContentType('text/plain');
  htp.add_CustomHeader('CUSTOM_HEADER: HEADER
CUSTOM_HEADER1: HEADER1
');
  htp.prn(ap);
  hrslt.ADD_FOOTER := false;
  dbms_lock.sleep(15);
  rollback;
end;`,
			procDrop:  ``,
			urlValues: url.Values{"ap": []string{"1"}},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  10 * time.Second,
			idleTimeout:  1 * time.Second,
			afterTimeout: 0 * time.Second,
			resCode:      StatusRequestWasInterrupted,
			resContent:   "",
		})
		wg.Done()
	}()

	go func() {
		//Пробуем прервать сессию, которой нет. Ошибки не должно быть
		err := Bkeak("/asrolf-ti11", "sess6-1")
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		err = Bkeak("/asrolf-ti11", "sess6")
		if err != nil {
			t.Fatal(err)
		}
		wg.Done()
	}()
	wg.Wait()
}
