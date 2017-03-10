package main

import (
	"flag"
	"net"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/jacobwpeng/sirius/frame"
	server "github.com/jacobwpeng/sirius/server/proto"
)

func MustMarshal(msg proto.Message) (data []byte) {
	data, err := proto.Marshal(msg)
	if err != nil {
		glog.Fatal(err)
	}
	return data
}

func main() {
	flag.Parse()
	raddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:9427")
	if err != nil {
		glog.Fatal(err)
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		glog.Fatal(err)
	}

	req := &server.GetRequest{
		Rank: proto.Uint32(1),
		Id:   proto.Uint64(2191195),
	}
	msgType := uint32(server.MessageType_TypeGetRequest)
	f := frame.NewFrame(msgType, MustMarshal(req))
	f.Ctx = 0xdeadbeef
	glog.Infof("0x%X", f.Magic)
	_, err = f.WriteTo(conn)
	if err != nil {
		glog.Fatal(err)
	}

	var replyFrame frame.Frame
	_, err = replyFrame.ReadFrom(conn)
	if err != nil {
		glog.Fatal(err)
	}
	glog.Info(replyFrame)
}
