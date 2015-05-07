package fluidbackup

import "net"
import "net/rpc"
import "fmt"
import "time"

/*
 * Local module for communication with peers.
 * Main responsibility is to forward things to other peers
 * on the network (through their respective protocol modules)
 */
type Protocol struct {
	peerList *PeerList
	rpc      *rpc.Server
	port     int
	l        net.Listener
}

/*
 * You need to init the protocol module first, here.
 */
func MakeProtocol(fluidBackup *FluidBackup, port int) *Protocol {
	this := new(Protocol)
	this.port = port

	this.rpc = rpc.NewServer()
	this.rpc.Register(this)
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
	this.l = l
	if e != nil {
		Log.Error.Printf("Error while initializing RPC handler: %s", e.Error())
		return nil
	}
	go func() {
		for !fluidBackup.Stopping() {
			conn, _ := this.l.Accept()

			if conn != nil {
				this.rpc.ServeConn(conn)
			}
		}
	}()

	return this
}

func (this *Protocol) Stop() {
	Log.Debug.Printf("Shutting down listener")
	this.l.Close()
}

/*
 * Shared communication structs
 */
type PingArgs struct {
	Me PeerId
}

type PingReply struct {
}

type ProposeAgreementArgs struct {
	Me        PeerId
	MyBytes   int
	YourBytes int
}

type ProposeAgreementReply struct {
	Accept bool
}

type StoreShardArgs struct {
	Me    PeerId
	Label int64
	Bytes []byte
}

type StoreShardReply struct {
	Confirm bool
}

type DeleteShardArgs struct {
	Me    PeerId
	Label int64
}

type DeleteShardReply struct {
}

type RetrieveShardArgs struct {
	Me    PeerId
	Label int64
}

type RetrieveShardReply struct {
	Bytes []byte
}

type ShareNewPeersArgs struct {
	Me  PeerId
	Num int
}

type ShareNewPeersReply struct {
	SharedPeers []PeerId
}

func (this *Protocol) setPeerList(peerList *PeerList) {
	this.peerList = peerList
}

/*
 * Get a reference to the current peer/protocol module
 */
func (this *Protocol) GetMe() PeerId {
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
					return PeerId{Address: addr.String(), Port: this.port}
				}
			}
		}
	}

	return PeerId{Address: "127.0.0.1", Port: this.port}
}

/*
 * RPC helper method
 */
func (this *Protocol) call(peerId PeerId, fn string, args interface{}, reply interface{}) bool {
	c := make(chan bool, 1)

	go func() {
		client, errx := rpc.Dial("tcp", peerId.String())
		if errx != nil {
			c <- false
			return
		}
		defer client.Close()

		err := client.Call("Protocol.Handle"+fn, args, reply)
		if err == nil {
			c <- true
			return
		}

		Log.Warn.Println(err)
		c <- false
	}()

	select {
		case b := <- c:
			return b
		case <- time.After(time.Second):
			return false
	}
}

/*
 * Ask another peer to see if it is alive.
 */
func (this *Protocol) ping(peerId PeerId) bool {
	args := &PingArgs{Me: this.GetMe()}
	var reply PingReply
	success := this.call(peerId, "Ping", args, &reply)
	return success
}

/*
 * Notify another peer that we want to store
 * a number of bytes in return for their storing a number
 * of our bytes.
 */
func (this *Protocol) proposeAgreement(peerId PeerId, localBytes int, remoteBytes int) bool {
	args := &ProposeAgreementArgs{
		Me:        this.GetMe(),
		MyBytes:   localBytes,
		YourBytes: remoteBytes,
	}
	var reply ProposeAgreementReply
	success := this.call(peerId, "ProposeAgreement", args, &reply)
	Log.Debug.Printf("ProposeAgreement %s (local: %d; remote: %d): %t", peerId.String(), localBytes, remoteBytes, success && reply.Accept)
	return success && reply.Accept
}

/*
 * Store a set of bytes with the given known peer
 */
func (this *Protocol) storeShard(peerId PeerId, label int64, bytes []byte) bool {
	args := &StoreShardArgs{
		Me:    this.GetMe(),
		Label: label,
		Bytes: bytes,
	}
	var reply StoreShardReply
	success := this.call(peerId, "StoreShard", args, &reply)
	this.peerList.UpdateTrustPostStorage(peerId, success && reply.Confirm)
	return success && reply.Confirm
}

func (this *Protocol) deleteShard(peerId PeerId, label int64) {
	args := &DeleteShardArgs{
		Me:    this.GetMe(),
		Label: label,
	}
	var reply DeleteShardReply
	this.call(peerId, "DeleteShard", args, &reply)
}

/*
 * Retrieve a labeled set of bytes from the given peer.
 */
func (this *Protocol) retrieveShard(peerId PeerId, label int64) []byte {
	args := &RetrieveShardArgs{
		Me:    this.GetMe(),
		Label: label,
	}
	var reply RetrieveShardReply
	success := this.call(peerId, "RetrieveShard", args, &reply)
	this.peerList.UpdateTrustPostRetrieval(peerId, success)
	if !success {
		return nil
	} else {
		return reply.Bytes
	}
}

/*
 * Called when another peer notifies us they exist
 */
func (this *Protocol) HandlePing(args *PingArgs, reply *PingReply) error {
	if this.peerList != nil {
		this.peerList.DiscoveredPeer(args.Me)
	}
	return nil
}

/*
 * Called when another peer notifies us they want to
 * create a storage agreement.
 */
func (this *Protocol) HandleProposeAgreement(args *ProposeAgreementArgs, reply *ProposeAgreementReply) error {
	if this.peerList == nil {
		reply.Accept = false
	} else {
		reply.Accept = this.peerList.HandleProposeAgreement(args.Me, args.YourBytes, args.MyBytes)
	}

	return nil
}

/*
 * Called from another peer when they want to
 * store a shard with us
 */
func (this *Protocol) HandleStoreShard(args *StoreShardArgs, reply *StoreShardReply) error {
	if this.peerList == nil {
		reply.Confirm = false
	} else {
		reply.Confirm = this.peerList.HandleStoreShard(args.Me, args.Label, args.Bytes)
	}

	return nil
}

/*
 * Called from another peer when they want to delete a previously stored shard.
 */
func (this *Protocol) HandleDeleteShard(args *DeleteShardArgs, reply *DeleteShardReply) error {
	if this.peerList != nil {
		this.peerList.HandleDeleteShard(args.Me, args.Label)
	}

	return nil
}

/*
 * Called from another peer when they want to retrieve
 * a shard from us.
 */
func (this *Protocol) HandleRetrieveShard(args *RetrieveShardArgs, reply *RetrieveShardReply) error {
	if this.peerList == nil {
		reply.Bytes = nil
	} else {
		reply.Bytes = this.peerList.HandleRetrieveShard(args.Me, args.Label)
	}

	return nil
}

/* ============== *
 * Peer Discovery *
 * ============== */

// Ask the given remote peer (id) to share
// some of its valuable peers
func (this *Protocol) askForPeers(peerId PeerId, num int) []PeerId {
	args := &ShareNewPeersArgs{}
	var reply ShareNewPeersReply
	args.Me = this.GetMe()
	args.Num = num
	success := this.call(peerId, "ShareNewPeers", args, &reply)
	if success {
		return reply.SharedPeers
	}
	return nil
}

/*
 * Request to share new peers...
 */
func (this *Protocol) HandleShareNewPeers(args *ShareNewPeersArgs, reply *ShareNewPeersReply) error {
	asker := args.Me
	peerList := this.peerList.HandleShareNewPeers(asker, args.Num)

	reply.SharedPeers = peerList
	return nil
}
