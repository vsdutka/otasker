// otasker_test
package otasker

import (
	"fmt"
	"testing"
)

func TestTasker(t *testing.T) {
	f := func(op *operation) {
		//fmt.Println(op)
	}
	taskerProc := newOwaClassicProcRunner()
	tasker := taskerProc(f, "--- 111 ---")
	res := tasker.Run("sessionID", "taskID", "a", "aaa111", "ROLF-TST14", "", "", "", "", nil, "root$.startup", nil, nil)
	if res.resStatusCode != 200 {
		t.Errorf("Status code should be \"%d\", was \"%d\"", 200, res.resStatusCode)
	}
	fmt.Println(tasker.Info())
}
