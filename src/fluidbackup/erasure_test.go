package fluidbackup

import "testing"
import "bytes"
import "math/rand"

func EncodeDecode(t *testing.T, k int, n int) {
	shardLength := 64
	size := k * shardLength

	original := make([]byte, size)
	for i := range original {
		original[i] = byte(rand.Int() % 256)
	}

	codeShards := erasureEncode(original, k, n)

	remainingShardsIndices := make([]int, k)
	remainingShardsMap := make(map[int]bool)
	remainingShards := make([][]byte, k)
	for i := range remainingShards {
		for {
			remainingShardsIndices[i] = rand.Int() % n
			_, alreadyUsed := remainingShardsMap[remainingShardsIndices[i]]
			if !alreadyUsed {
				break
			}
		}

		remainingShards[i] = codeShards[remainingShardsIndices[i]]
		remainingShardsMap[remainingShardsIndices[i]] = true
	}

	recoveredBytes := erasureDecode(remainingShards, k, n, remainingShardsIndices)
	if !bytes.Equal(original, recoveredBytes) {
		t.Error("recovered data does not match original")
	}
}

func TestEncodeDecodeTrivial(t *testing.T) {
	EncodeDecode(t, 3, 3)
}

func TestEncodeDecodeActual(t *testing.T) {
	EncodeDecode(t, 12, 16)
}
