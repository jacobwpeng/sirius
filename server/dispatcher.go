package server

import (
	"sync"

	"github.com/golang/glog"
	"github.com/jacobwpeng/sirius/engine"
)

type Dispatcher struct {
	wg           sync.WaitGroup
	doneChan     chan struct{}
	rankHandlers map[uint32]*RankHandler
	jobQueue     chan Job
}

func NewDispatcher(ranks map[uint32]engine.RankEngine) *Dispatcher {
	rankHandlers := make(map[uint32]*RankHandler)
	for rankID, rank := range ranks {
		rankHandlers[rankID] = NewRankHandler(rankID, rank)
	}
	return &Dispatcher{
		doneChan:     make(chan struct{}),
		rankHandlers: rankHandlers,
		jobQueue:     make(chan Job, MAX_BUFFERED_JOB),
	}
}

func (d *Dispatcher) AddRank(rankID uint32, rank engine.RankEngine) bool {
	_, exist := d.rankHandlers[rankID]
	if exist {
		return false
	}
	d.rankHandlers[rankID] = NewRankHandler(rankID, rank)
	return true
}

func (d *Dispatcher) Start() {
	for _, handler := range d.rankHandlers {
		d.wg.Add(1)
		handler.Start(&d.wg)
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		for {
			select {
			case <-d.doneChan:
				glog.V(2).Info("Dispatcher exit")
				return
			case job := <-d.jobQueue:
				glog.V(2).Info("New job in dispatcher")
				rankHandler, exist := d.rankHandlers[job.RankID]
				if !exist {
					glog.Infof("Rank %d not exist", job.RankID)
					job.resultChan <- JobResult{
						FrameCtx: job.Frame.Ctx,
						ErrCode:  ErrRankNotFound,
					}
					continue
				}
				rankHandler.jobQueue <- job
			}
		}
	}()
}

func (d *Dispatcher) Stop() {
	close(d.doneChan)
	for _, handler := range d.rankHandlers {
		handler.Stop()
	}
	d.wg.Wait()
}
