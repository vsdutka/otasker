// worker
package otasker

import (
	"net/url"
	"strings"
	"sync"
	"time"
)

type work struct {
	sessionID         string
	taskID            string
	reqUserName       string
	reqUserPass       string
	reqConnStr        string
	reqParamStoreProc string
	reqBeforeScript   string
	reqAfterScript    string
	reqDocumentTable  string
	reqCGIEnv         map[string]string
	reqProc           string
	reqParams         url.Values
	reqFiles          *Form
	dumpFileName      string
}

type worker struct {
	oracleTasker
	sync.RWMutex
	signalChan  chan string
	inChan      chan work
	outChanList map[string]chan OracleTaskResult
	startedAt   time.Time
	started     bool
}

func (w *worker) start() {
	w.Lock()
	w.startedAt = time.Now()
	w.started = true
	w.Unlock()
}

func (w *worker) finish() {
	w.Lock()
	w.startedAt = time.Time{}
	w.started = false
	w.Unlock()
	w.signalChan <- ""
}

func (w *worker) outChan(taskID string) (chan OracleTaskResult, bool) {
	w.RLock()
	defer w.RUnlock()
	c, ok := w.outChanList[taskID]
	return c, ok
}

func (w *worker) worked() int64 {
	w.RLock()
	defer w.RUnlock()
	if w.started {
		return int64(time.Since(w.startedAt) / time.Second)
	}
	return 0
}

func (w *worker) listen(ID string, idleTimeout time.Duration) {
	w.signalChan <- ""
	defer func() {
		// Удаляем данный обработчик из списка доступных
		wlock.Lock()
		delete(wlist, ID)
		wlock.Unlock()
		w.CloseAndFree()
	}()

	for {
		select {
		case wrk := <-w.inChan:
			{
				w.start()

				res := func() OracleTaskResult {
					return w.Run(wrk.sessionID,
						wrk.taskID,
						wrk.reqUserName,
						wrk.reqUserPass,
						wrk.reqConnStr,
						wrk.reqParamStoreProc,
						wrk.reqBeforeScript,
						wrk.reqAfterScript,
						wrk.reqDocumentTable,
						wrk.reqCGIEnv,
						wrk.reqProc,
						wrk.reqParams,
						wrk.reqFiles,
						wrk.dumpFileName)
				}()
				outChan, ok := w.outChan(wrk.taskID)
				if ok {
					outChan <- res
				}
				w.finish()
				if res.StatusCode == StatusRequestWasInterrupted {
					return
				}

			}
		case <-time.After(idleTimeout):
			{
				return
			}
		}
	}
}

var (
	wlock sync.RWMutex
	wlist = make(map[string]map[string]*worker)
)

func Run(
	path,
	sessionID,
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
	fn func() oracleTasker,
	waitTimeout, idleTimeout time.Duration,
	dumpFileName string,
) OracleTaskResult {
	w := func() *worker {
		wlock.RLock()
		w, ok := wlist[strings.ToUpper(path)][strings.ToUpper(sessionID)]
		wlock.RUnlock()
		if !ok {
			wlock.Lock()
			defer wlock.Unlock()

			w = &worker{
				oracleTasker: fn(),
				signalChan:   make(chan string, 1),
				inChan:       make(chan work),
				outChanList:  make(map[string]chan OracleTaskResult),
				startedAt:    time.Time{},
				started:      false,
			}
			go w.listen(strings.ToUpper(sessionID), idleTimeout)
			wlist[strings.ToUpper(path)][strings.ToUpper(sessionID)] = w
		}
		return w
	}()

	// Проверяем, если результаты по задаче
	outChan, ok := w.outChan(taskID)
	if !ok {
		//Если еще не было отправки, то проверяем на то, что можно отправит
		select {
		case <-w.signalChan:
			{
				//Удалось прочитать сигнал о незанятости вокера. Шлем задачу в него
				wrk := work{
					sessionID:         sessionID,
					taskID:            taskID,
					reqUserName:       userName,
					reqUserPass:       userPass,
					reqConnStr:        connStr,
					reqParamStoreProc: paramStoreProc,
					reqBeforeScript:   beforeScript,
					reqAfterScript:    afterScript,
					reqDocumentTable:  documentTable,
					reqCGIEnv:         cgiEnv,
					reqProc:           procName,
					reqParams:         urlParams,
					reqFiles:          reqFiles,
					dumpFileName:      dumpFileName,
				}
				outChan = make(chan OracleTaskResult, 1)
				w.Lock()
				w.outChanList[taskID] = outChan
				w.Unlock()
				w.inChan <- wrk

			}
		case <-time.After(waitTimeout):
			{
				/* Сигнализируем о том, что идет выполнение другог запроса и нужно предложить прервать */
				return OracleTaskResult{StatusCode: StatusBreakPage, Duration: w.worked()}
			}
		}
	}
	//Читаем результаты
	return func() OracleTaskResult {
		select {
		case res := <-outChan:
			return res
		case <-time.After(waitTimeout):
			{
				/* Сигнализируем о том, что идет выполнение этого запроса и нужно показать червяка */
				return OracleTaskResult{StatusCode: StatusWaitPage, Duration: w.worked()}
			}
		}
	}()
}

func Bkeak(path, sessionID string) error {
	wlock.RLock()
	w, ok := wlist[strings.ToUpper(path)][strings.ToUpper(sessionID)]
	wlock.RUnlock()
	if !ok {
		return nil
	}
	return w.Break()
}

//func main() {
//	readyChan := make(chan bool)
//	dataChan := make(chan int)

//	go func() {
//		readyChan <- true
//		for {
//			select {
//			case t1 := <- dataChan:
//				{
//					fmt.Println(t1)
//					time.Sleep(10*time.Second)
//					readyChan <- true
//				}
//			/*case <-time.After(10 * time.Second):
//				{
//					fmt.Println("timeout")
//				}*/
//			}
//		}
//	}()
//	select {
//	case <-readyChan:
//	{
//		dataChan<-20
//	}
//	case  <-time.After(8 * time.Second):
//				{
//					fmt.Println("timeout 1")
//				}
//	}

//	select {
//	case <-readyChan:
//	{
//		dataChan<-30
//	}
//	case  <-time.After(10 * time.Second):
//				{
//					fmt.Println("timeout 1")
//				}
//	}
//	<-time.After(20 * time.Second)
//	fmt.Println("Exit")
//}
