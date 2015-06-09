// otasker_test
package otasker

import (
	//"fmt"
	"net/url"
	"sync"
	"testing"
	"time"
)

func TestTaskerRoot(t *testing.T) {
	f := func(op *OracleOperation) {
		//fmt.Println(op)
	}
	taskerProc := newOwaClassicProcRunner()
	tasker := taskerProc(f, "--- 111 ---")
	res := tasker.Run("sessionID", "taskID", "a", "aaa111", "ROLF-TST1", "", "", "", "", nil, "root$.startup", nil, nil)

	if res.StatusCode != 200 {
		t.Errorf("Status code should be \"%d\", was \"%d\"", 200, res.StatusCode)
	}
}

func TestTaskerBreak(t *testing.T) {
	f := func(op *OracleOperation) {
		//fmt.Println(op)
	}

	var wg sync.WaitGroup

	taskerProc := newOwaClassicProcRunner()
	tasker := taskerProc(f, "--- 111 ---")

	wg.Add(1)

	go func(tsk OracleTasker) {
		defer wg.Done()
		p := make(url.Values)

		p.Set("A_AG_ID", "15953003")
		p.Set("A_PT_DC_ID", "9")
		p.Set("A_CRRNCY_ID", "2")
		res := tsk.Run("sessionID", "taskID", "a", "aaa111", "ROLF-TST1", "", "", "", "", nil, "SRV$S", p, nil)
		//fmt.Println(string(res.Content))
		if res.StatusCode != StatusRequestWasInterrupted {
			t.Errorf("Status code should be \"%d\", was \"%d\"", StatusRequestWasInterrupted, res.StatusCode)
		}

		res = tsk.Run("sessionID", "taskID", "a", "aaa111", "ROLF-TST1", "", "", "", "", nil, "toot$.startup", nil, nil)
		//fmt.Println(string(res.Content))
		if res.StatusCode != 200 {
			t.Errorf("Status code should be \"%d\", was \"%d\"", 200, res.StatusCode)
		}
	}(tasker)
	<-time.After(5 * time.Second)
	//fmt.Println(tasker.Info())
	tasker.Break()
	wg.Wait()
}
