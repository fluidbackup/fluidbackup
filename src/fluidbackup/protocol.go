package fluidbackup

import "net"
import "net/rpc"
import "net/http"
import "strconv"

type Protocol struct {
	peerList *PeerList
}

func MakeProtocol() *Protocol {
	this := new(Protocol)

	rpc.Register(this)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":19898")
	if e != nil {
		Log.Error.Printf("Error while initializing RPC handler: %s", e.Error())
		return nil
	}
	go http.Serve(l, nil)

	return this
}

type ProposeAgreementArgs struct {
	Me PeerId
	MyBytes int
	YourBytes int
}

type ProposeAgreementReply struct {
	Accept bool
}

type StoreShardArgs struct {
	Me PeerId
	Label string
	Bytes []byte
}

type StoreShardReply struct {
	Confirm bool
}

func (this *Protocol) getMe() PeerId {
	ifaces, err := net.Interfaces()

	if err != nil {
		Log.Warn.Println(err)
	}

	for _, i := range ifaces {
		addrs, err := i.Addrs()

		if err == nil {
			for _, addr := range addrs {
				switch addr.(type) {
				case *net.IPAddr:
					return PeerId{Address: addr.String(), Port: 19898}
				}
			}
		}
	}

	return PeerId{Address: "127.0.0.1", Port: 19898}
}

func (this *Protocol) call(peerId PeerId, fn string, args interface{}, reply interface{}) bool {
	c, errx := rpc.DialHTTP("tcp", peerId.Address + ":" + strconv.Itoa(peerId.Port))
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call("Protocol.Handle" + fn, args, reply)
	if err == nil {
		return true
	}

	Log.Warn.Println(err)
	return false
}

func (this *Protocol) proposeAgreement(peerId PeerId, localBytes int, remoteBytes int) bool {
	args := &ProposeAgreementArgs{
		Me: this.getMe(),
		MyBytes: localBytes,
		YourBytes: remoteBytes,
	}
	var reply ProposeAgreementReply
	success := this.call(peerId, "ProposeAgreement", args, &reply)
	return success && reply.Accept
}

func (this *Protocol) storeShard(peerId PeerId, label string, bytes []byte) bool {
	args := &StoreShardArgs{
		Me: this.getMe(),
		Label: label,
		Bytes: bytes,
	}
	var reply StoreShardReply
	success := this.call(peerId, "StoreShard", args, &reply)
	return success && reply.Confirm
}
