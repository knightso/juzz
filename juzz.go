package juzz

import (
	"reflect"
	"sync"
	"time"

	"lodeo.dynamic/util/logs"
)

type StartFunc func() (string, interface{}, error)
type WorkerFunc func(ctx interface{}) (string, interface{}, error)

type Finalizable interface {
	Finalize() error
}

type Worker struct {
	name         string
	numReferers  int
	in           chan interface{}
	fn           WorkerFunc
	nexts        map[string]*Worker
	multiplicity int
	sync.Mutex
}

func (w *Worker) Next(next *Worker) *Worker {
	w.NextFor("", next)
	return next
}

func (w *Worker) Nexts(nexts map[string]*Worker) {
	for k, v := range nexts {
		w.NextFor(k, v)
	}
}

func (w *Worker) NextFor(result string, next *Worker) {
	if w.nexts == nil {
		w.nexts = make(map[string]*Worker)
	}
	next.numReferers++
	w.nexts[result] = next
}

func (w *Worker) run(wswg *sync.WaitGroup) {
	wswg.Add(1)
	go func() {
		defer wswg.Done()

		wg := &sync.WaitGroup{}
		for i := 0; i < w.multiplicity; i++ {
			i := i

			wg.Add(1)
			go func() {
				defer wg.Done()
				w.runSingle(i)
			}()
		}
		wg.Wait()

		for _, next := range w.nexts {
			next := next

			func() {
				next.Lock()
				defer next.Unlock()

				next.numReferers--
				if next.numReferers <= 0 {
					close(next.in)
				}
			}()
		}
	}()
}

func (w *Worker) runSingle(index int) {
	for ctx := range w.in {
		func(ctx interface{}) {
			cancelFinalize := false
			if f, ok := ctx.(Finalizable); ok {
				defer func() {
					if !cancelFinalize {
						if err := f.Finalize(); err != nil {
							logs.Warn.Printf("context finalize error. %v", err)
						}
					}
				}()
			}

			logs.Info.Printf("processing %s[%d]...", w.name, index)
			start := time.Now()

			result, ctx, err := w.fn(ctx)
			if err != nil {
				logs.Error.Printf("failed %s[%d]. err:%v", w.name, index, err)
				return
			}
			if ctx == nil || reflect.ValueOf(ctx).IsNil() {
				// if fn returns null context, end process.
				return
			}

			logs.Info.Printf("%s[%d] done. time:%f", w.name, index, time.Now().Sub(start).Seconds())

			if w.nexts != nil {
				if next, ok := w.nexts[result]; ok {
					cancelFinalize = true
					next.in <- ctx
				} else {
					logs.Warn.Printf("%s[%d] next worker not found: %s", w.name, index, result)
				}
			}
		}(ctx)
	}
	logs.Info.Printf("%s[%d] was shutdown.", w.name, index)
}

type Workers struct {
	starter    *Worker
	workers    []*Worker
	shutdownCh chan struct{}
	sync.WaitGroup
}

func (ws *Workers) NewStarter(name string, fn StartFunc, multiplicity int) *Worker {
	if ws.starter != nil {
		panic("NewStarter can be called only once.")
	}

	wfn := func(_ interface{}) (string, interface{}, error) {
		return fn()
	}
	ws.starter = ws.NewWorker(name, wfn, multiplicity)
	return ws.starter
}

func (ws *Workers) NewWorker(name string, fn WorkerFunc, multiplicity int) *Worker {
	w := &Worker{name: name, fn: fn, multiplicity: multiplicity}
	w.in = make(chan interface{})
	ws.workers = append(ws.workers, w)
	return w
}

func (ws *Workers) Run() {
	if ws.starter == nil {
		panic("starter required.")
	}

	if ws.shutdownCh != nil {
		panic("already Ran")
	}

	ws.shutdownCh = make(chan struct{})

	for _, w := range ws.workers {
		w.run(&ws.WaitGroup)
	}

	go func() {
	L:
		for {
			select {
			case <-ws.shutdownCh:
				close(ws.starter.in)
				break L
			case ws.starter.in <- nil:
			}
		}
	}()
}

func (ws *Workers) Shutdown() {
	ws.shutdownCh <- struct{}{}
	ws.Wait()
}
