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

func (w *Worker) SetNext(next *Worker) *Worker {
	w.SetNexts(map[string]*Worker{"": next})
	return next
}

func (w *Worker) SetNexts(nexts map[string]*Worker) {
	w.nexts = nexts

	for _, next := range nexts {
		next.numReferers++
	}
}

func (w *Worker) Run() {
	go func() {
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
				if next.numReferers == 0 {
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
}

type Workers struct {
	starter *Worker
	workers []*Worker
	closeCh chan struct{}
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

	if ws.closeCh != nil {
		panic("already Ran")
	}

	ws.closeCh = make(chan struct{})

	for _, w := range ws.workers {
		w.Run()
	}

	go func() {
	L:
		for {
			select {
			case <-ws.closeCh:
				close(ws.starter.in)
				break L
			case ws.starter.in <- nil:
			}
		}
	}()
}

func (ws *Workers) Close(timeout time.Duration) {
	ws.closeCh <- struct{}{}

	// sleep for now
	// TODO: wait for the last channel close
	time.Sleep(timeout)
}
