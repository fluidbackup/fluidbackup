package fluidbackup

type Protocol struct {

}

func (this *Protocol) proposeAgreement(peerId PeerId, localBytes int, remoteBytes int) bool {
	return false
}
