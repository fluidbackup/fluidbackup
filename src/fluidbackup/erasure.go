package fluidbackup

import "github.com/somethingnew2-0/go-erasure"

/*
 * erasureEncode converts a given block of data into N code shards each of size len(data)/K,
 *  such that the block can be recovered with any K of the code shards.
 * This operation may not work correctly in conjuction with erasureDecode if len(data) is not divisible by K.
 */
func erasureEncode(data []byte, k int, n int) [][]byte {
	shardLen := len(data) / k
	var encodedBlob []byte
	if n > k {
		code := erasure.NewCode(n, k, len(data))
		encodedBlob = code.Encode(data)
	}

	shards := make([][]byte, n)
	for i := 0; i < n; i++ {
		if i < k {
			shards[i] = data[i * shardLen : (i+1) * shardLen]
		} else {
			shards[i] = encodedBlob[(i-k) * shardLen : (i-k+1) * shardLen]
		}
	}

	return shards
}

/*
 * Decodes data encoded by erasureEncode and returns original data block.
 * shards should contain exactly K code shards that are still available.
 * n, k match those in the erasureEncode call.
 * chunks identifies the indices of the provided code shards in the original slice returned by erasureEncode.
 */
func erasureDecode(shards [][]byte, k int, n int, chunks []int) []byte {
	shardLen := len(shards[0])

	if n > k {
		code := erasure.NewCode(n, k, shardLen * k)

		encodedBlob := make([]byte, 0, shardLen * n)
		errList := make([]byte, 0)
		zeros := make([]byte, shardLen)

		for i := 0; i < n; i++ {
			// use the code shard from code parameter if set in chunks, otherwise fill with zeroes
			found := false
			for codeIndex, shardIndex := range chunks {
				if shardIndex == i {
					encodedBlob = append(encodedBlob, shards[codeIndex]...)
					found = true
					break
				}
			}

			if !found {
				encodedBlob = append(encodedBlob, zeros...)
				errList = append(errList, byte(i))
			}
		}

		return code.Decode(encodedBlob, errList, false)
	} else {
		data := make([]byte, 0, shardLen * k)

		for i := 0; i < k; i++ {
			for codeIndex, shardIndex := range chunks {
				if shardIndex == i {
					data = append(data, shards[codeIndex]...)
					break
				}
			}
		}

		return data
	}
}
