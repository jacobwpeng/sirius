package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/jacobwpeng/sirius/engine"
	"github.com/jacobwpeng/sirius/serverproto"
)

const (
	MAX_BUFFERED_JOB = 128
)

type RankHandler struct {
	primaryRankID uint32
	primaryRank   engine.RankEngine
	snapshotRanks map[uint32]engine.RankEngine
	done          chan struct{}
	jobQueue      chan Job
}

func NewRankHandler(rankID uint32, rank engine.RankEngine) *RankHandler {
	return &RankHandler{
		primaryRankID: rankID,
		primaryRank:   rank,
		done:          make(chan struct{}),
		jobQueue:      make(chan Job, MAX_BUFFERED_JOB),
		snapshotRanks: make(map[uint32]engine.RankEngine),
	}
}

func (h *RankHandler) AddSnapshotRank(rankID uint32,
	rank engine.RankEngine) error {
	if rank.Config().PrimaryRankID != h.primaryRankID {
		return fmt.Errorf("Expect primary rank id %d, got: %d",
			h.primaryRankID, rank.Config().PrimaryRankID)
	}
	_, exist := h.snapshotRanks[rankID]
	if exist {
		return fmt.Errorf("Snapshot rank %d already exist", rankID)
	}
	h.snapshotRanks[rankID] = rank
	return nil
}

func RankUnitToProto(u engine.RankUnit) *serverproto.RankUnit {
	return &serverproto.RankUnit{
		Id:  proto.Uint64(u.ID),
		Key: proto.Uint64(u.Key),
	}
}

func RankUnitFromProto(u *serverproto.RankUnit) engine.RankUnit {
	return engine.RankUnit{
		ID:    u.GetId(),
		Key:   u.GetKey(),
		Value: u.GetValue(),
	}
}

func (h *RankHandler) Start(wg *sync.WaitGroup) {
	const CRON_CHECK_INTERVAL time.Duration = time.Millisecond * 500
	c := time.NewTicker(CRON_CHECK_INTERVAL).C
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case job := <-h.jobQueue:
				h.HandleJob(job)
			case <-h.done:
				glog.Infof("RankHandler %d exit", h.primaryRankID)
				return
			case now := <-c:
				h.CronCheckAllRanks(now)
			}
		}
	}()
}

func (h *RankHandler) Stop() {
	close(h.done)
}

func (h *RankHandler) CronCheckAllRanks(now time.Time) {
	glog.V(2).Infof("CronCheckAllRanks, now: %v", now)
	for rankID, rank := range h.snapshotRanks {
		glog.V(2).Infof("Check snapshot rank rankID: %d", rankID)
		h.MaybeClearRank(rank, now)
		h.MaybeSnapshotRank(rank, now)
	}
	glog.V(2).Infof("Check snapshot ranks done, now: %v", now)
	h.MaybeClearRank(h.primaryRank, now)
	glog.V(2).Infof("CronCheckAllRanks done, now: %v", now)
}

func (h *RankHandler) FindRank(rankID uint32) engine.RankEngine {
	if rankID == h.primaryRankID {
		return h.primaryRank
	}
	rank, _ := h.snapshotRanks[rankID]
	return rank
}

func (h *RankHandler) HandleJob(job Job) {
	glog.V(2).Infof("Rank: %d, Ctx: %d", job.RankID, job.Frame.Ctx)
	rank := h.FindRank(job.RankID)
	if rank == nil {
		glog.Fatalf("Rank %d not found!")
	}
	now := time.Now()
	if job.RankID == h.primaryRankID {
		h.MaybeSnapshotPrimaryRank(now)
	} else {
		h.MaybeSnapshotRank(rank, now)
	}
	h.MaybeClearRank(rank, now)
	var jobResult JobResult
	switch msg := job.Msg.(type) {
	case *serverproto.GetRequest:
		jobResult = h.HandleGet(job, rank, msg)
	case *serverproto.GetByRankRequest:
		jobResult = h.HandleGetByRank(job, rank, msg)
	case *serverproto.GetRangeRequest:
		jobResult = h.HandleGetRange(job, rank, msg)
	case *serverproto.UpdateRequest:
		jobResult = h.HandleUpdate(job, rank, msg)
		if !msg.GetReply() {
			return
		}
	case *serverproto.DeleteRequest:
		jobResult = h.HandleDelete(job, rank, msg)
		if !msg.GetReply() {
			return
		}
	default:
		glog.Infof("Unexpected msg type %d", job.Frame.PayloadType)
	}
	glog.V(2).Infof("Write job result, FrameCtx: %d", job.Frame.Ctx)
	job.resultChan <- jobResult
}

func (h *RankHandler) MaybeSnapshotPrimaryRank(now time.Time) {
	for _, rank := range h.snapshotRanks {
		h.MaybeSnapshotRank(rank, now)
	}
}

func (h *RankHandler) MaybeSnapshotRank(rank engine.RankEngine, now time.Time) {
	if rank.Config().SnapshotPeriod.Empty() {
		return
	}
	lastTime := rank.LastSnapshotTime()
	nextTime := rank.Config().SnapshotPeriod.NextTime(lastTime)
	if now.Before(nextTime) {
		return
	}
	rank.CopyFrom(h.primaryRank)
	rank.SetLastSnapshotTime(now)
	glog.Infof("Snapshot rank, last: %v, next: %v, now: %v",
		lastTime, nextTime, now)
}

func (h *RankHandler) MaybeClearRank(rank engine.RankEngine, now time.Time) {
	if rank.Config().ClearPeriod.Empty() {
		return
	}
	lastTime := rank.LastClearTime()
	nextTime := rank.Config().ClearPeriod.NextTime(lastTime)
	if now.Before(nextTime) {
		return
	}
	rank.Clear()
	rank.SetLastClearTime(now)
	glog.Infof("Clear rank, last: %v, next: %v, now: %v", lastTime, nextTime, now)
}

func (h *RankHandler) HandleGet(job Job, rank engine.RankEngine,
	msg *serverproto.GetRequest) JobResult {
	_, pos, value := rank.Get(msg.GetId())

	resp := &serverproto.GetResponse{
		Rank: proto.Uint32(job.RankID),
		Pos:  proto.Uint32(pos),
		Data: RankUnitToProto(value),
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(serverproto.MessageType_TypeGetResponse),
		Msg:              resp,
	}
}

func (h *RankHandler) HandleGetByRank(job Job, rank engine.RankEngine,
	msg *serverproto.GetByRankRequest) JobResult {
	exist, value := rank.GetByRank(msg.GetPos())
	var pos uint32
	if exist {
		pos = msg.GetPos()
	}

	resp := &serverproto.GetByRankResponse{
		Rank: proto.Uint32(job.RankID),
		Pos:  proto.Uint32(pos),
		Data: RankUnitToProto(value),
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(serverproto.MessageType_TypeGetByRankResponse),
		Msg:              resp,
	}
}

func (h *RankHandler) HandleGetRange(job Job, rank engine.RankEngine,
	msg *serverproto.GetRangeRequest) JobResult {
	values := rank.GetRange(msg.GetStart(), msg.GetNum())
	data := make([]*serverproto.RankUnit, len(values))
	for i, u := range values {
		data[i] = RankUnitToProto(u)
	}

	resp := &serverproto.GetRangeResponse{
		Rank:  proto.Uint32(job.RankID),
		Total: proto.Uint32(rank.Size()),
		Data:  data,
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(serverproto.MessageType_TypeGetRangeResponse),
		Msg:              resp,
	}
}

func (h *RankHandler) HandleUpdate(job Job, rank engine.RankEngine,
	msg *serverproto.UpdateRequest) (res JobResult) {

	//TODO(jacobwpeng): 更高效的实现
	_, lastPos, lastData := rank.Get(msg.Data.GetId())
	rank.Update(RankUnitFromProto(msg.Data))
	_, pos, _ := rank.Get(msg.Data.GetId())
	if !msg.GetReply() {
		return res
	}
	resp := &serverproto.UpdateResponse{
		Rank:    proto.Uint32(job.RankID),
		LastPos: proto.Uint32(lastPos),
		Pos:     proto.Uint32(pos),
	}
	if msg.GetLastData() {
		resp.Data = RankUnitToProto(lastData)
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(serverproto.MessageType_TypeUpdateResponse),
		Msg:              resp,
	}
}

func (h *RankHandler) HandleDelete(job Job, rank engine.RankEngine,
	msg *serverproto.DeleteRequest) (res JobResult) {

	_, lastPos, lastData := rank.Delete(msg.GetId())
	if !msg.GetReply() {
		return res
	}
	resp := &serverproto.DeleteResponse{
		Rank:    proto.Uint32(job.RankID),
		LastPos: proto.Uint32(lastPos),
	}
	if msg.GetLastData() {
		resp.Data = RankUnitToProto(lastData)
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(serverproto.MessageType_TypeDeleteResponse),
		Msg:              resp,
	}
}
