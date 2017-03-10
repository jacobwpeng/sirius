package server

import (
	"sync"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/jacobwpeng/sirius/engine"
	server "github.com/jacobwpeng/sirius/server/proto"
)

const (
	MAX_BUFFERED_JOB = 128
)

type RankHandler struct {
	rankID   uint32
	rank     engine.RankEngine
	done     chan struct{}
	jobQueue chan Job
}

func NewRankHandler(rankID uint32, rank engine.RankEngine) *RankHandler {
	return &RankHandler{
		rankID:   rankID,
		rank:     rank,
		done:     make(chan struct{}),
		jobQueue: make(chan Job, MAX_BUFFERED_JOB),
	}
}

func RankUnitToProto(u engine.RankUnit) *server.RankUnit {
	return &server.RankUnit{
		Id:  proto.Uint64(u.ID),
		Key: proto.Uint64(u.Key),
	}
}

func RankUnitFromProto(u *server.RankUnit) engine.RankUnit {
	return engine.RankUnit{
		ID:    u.GetId(),
		Key:   u.GetKey(),
		Value: u.GetValue(),
	}
}

func (h *RankHandler) Start(wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		defer glog.Infof("RankHandler %d exit", h.rankID)
		for {
			select {
			case job := <-h.jobQueue:
				h.HandleJob(job)
			case <-h.done:
				return
			}
		}
	}()
}

func (h *RankHandler) Stop() {
	close(h.done)
}

func (h *RankHandler) HandleJob(job Job) {
	glog.V(2).Infof("FrameCtx: %d", job.Frame.Ctx)
	var jobResult JobResult
	switch msg := job.Msg.(type) {
	case *server.GetRequest:
		jobResult = h.HandleGet(job, msg)
	case *server.GetByRankRequest:
		jobResult = h.HandleGetByRank(job, msg)
	case *server.GetRangeRequest:
		jobResult = h.HandleGetRange(job, msg)
	case *server.UpdateRequest:
		jobResult = h.HandleUpdate(job, msg)
		if !msg.GetReply() {
			return
		}
	case *server.DeleteRequest:
		jobResult = h.HandleDelete(job, msg)
		if !msg.GetReply() {
			return
		}
	default:
		glog.Infof("Unexpected msg type %d", job.Frame.PayloadType)
	}
	glog.V(2).Infof("Write job result, FrameCtx: %d", job.Frame.Ctx)
	job.resultChan <- jobResult
}

func (h *RankHandler) HandleGet(job Job,
	msg *server.GetRequest) JobResult {
	_, pos, value := h.rank.Get(msg.GetId())

	resp := &server.GetResponse{
		Rank: proto.Uint32(h.rankID),
		Pos:  proto.Uint32(pos),
		Data: RankUnitToProto(value),
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(server.MessageType_TypeGetResponse),
		Msg:              resp,
	}
}

func (h *RankHandler) HandleGetByRank(job Job,
	msg *server.GetByRankRequest) JobResult {
	exist, value := h.rank.GetByRank(msg.GetPos())
	var pos uint32
	if exist {
		pos = msg.GetPos()
	}

	resp := &server.GetByRankResponse{
		Rank: proto.Uint32(h.rankID),
		Pos:  proto.Uint32(pos),
		Data: RankUnitToProto(value),
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(server.MessageType_TypeGetByRankResponse),
		Msg:              resp,
	}
}

func (h *RankHandler) HandleGetRange(job Job,
	msg *server.GetRangeRequest) JobResult {
	values := h.rank.GetRange(msg.GetStart(), msg.GetNum())
	data := make([]*server.RankUnit, len(values))
	for i, u := range values {
		data[i] = RankUnitToProto(u)
	}

	resp := &server.GetRangeResponse{
		Rank:  proto.Uint32(h.rankID),
		Total: proto.Uint32(h.rank.Size()),
		Data:  data,
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(server.MessageType_TypeGetRangeResponse),
		Msg:              resp,
	}
}

func (h *RankHandler) HandleUpdate(job Job,
	msg *server.UpdateRequest) (res JobResult) {

	//TODO(jacobwpeng): 更高效的实现
	_, lastPos, lastData := h.rank.Get(msg.Data.GetId())
	h.rank.Update(RankUnitFromProto(msg.Data))
	_, pos, _ := h.rank.Get(msg.Data.GetId())
	if !msg.GetReply() {
		return res
	}
	resp := &server.UpdateResponse{
		Rank:    proto.Uint32(h.rankID),
		LastPos: proto.Uint32(lastPos),
		Pos:     proto.Uint32(pos),
	}
	if msg.GetLastData() {
		resp.Data = RankUnitToProto(lastData)
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(server.MessageType_TypeUpdateResponse),
		Msg:              resp,
	}
}

func (h *RankHandler) HandleDelete(job Job,
	msg *server.DeleteRequest) (res JobResult) {

	_, lastPos, lastData := h.rank.Delete(msg.GetId())
	if !msg.GetReply() {
		return res
	}
	resp := &server.DeleteResponse{
		Rank:    proto.Uint32(h.rankID),
		LastPos: proto.Uint32(lastPos),
	}
	if msg.GetLastData() {
		resp.Data = RankUnitToProto(lastData)
	}
	return JobResult{
		FrameCtx:         job.Frame.Ctx,
		FramePayloadType: uint32(server.MessageType_TypeDeleteResponse),
		Msg:              resp,
	}
}
