// stat_test
package otasker

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestStatCollect(t *testing.T) {
	for i := 0; i < 10; i++ {
		workerRun(t, test{
			name:      fmt.Sprintf("%03d", i),
			sessionID: fmt.Sprintf("sess%03d", 10-i),
			taskID:    fmt.Sprintf("TASK%03d", i),
			userName:  user,
			userPass:  password,
			connStr:   connstr,
			procName:  fmt.Sprintf(`TestWorkerRun%d`, i),
			procCreate: fmt.Sprintf(`
create or replace procedure TestWorkerRun%d(ap in varchar2) is 
begin
  htp.prn(ap);
  hrslt.ADD_FOOTER := false;
  rollback;
end;`, i),
			procDrop:  fmt.Sprintf(`drop procedure TestWorkerRun%d`, i),
			urlValues: url.Values{"ap": []string{"1"}},
			files: &Form{
				Value: map[string][]string{},
				File:  map[string][]*FileHeader{},
			},
			waitTimeout:  10 * time.Second,
			idleTimeout:  15 * time.Second,
			afterTimeout: 0 * time.Second,
			resCode:      200,
			resContent:   "1",
		})
	}

	res := Collect("/asrolf-ti11", "HandlerID")
	if len(res) < 10 {
		t.Fatalf("%s: got \"%v\",\nwant \"%v\"", "StatCollect: Len - ", len(res), 10)
	}

	if res[0].MessageID != "TASK009" {
		t.Fatalf("%s: got \"%v\",\nwant \"%v\"", "StatCollect: res[0].MessageID", res[0].MessageID, "TASK009")
	}
	if res[9].MessageID != "TASK000" {
		t.Fatalf("%s: got \"%v\",\nwant \"%v\"", "StatCollect: res[9].MessageID", res[0].MessageID, "TASK000")
	}
}
