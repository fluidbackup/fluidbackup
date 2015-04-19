package fluidbackup

/*
 * erasureEncode converts a given block of data into N code shards each of size len(data)/K,
 *  such that the block can be recovered with any K of the code shards.
 */
func erasureEncode(data []byte, k int, n int) [][]byte {
	return nil
}

/*
 * Decodes data encoded by erasureEncode and returns original data block.
 * code should contain exactly K code shards that are still available.
 * n, k match those in the erasureEncode call.
 * chunks identifies the indices of the provided code shards in the original slice returned by erasureEncode.
 */
func erasureDecode(code [][]byte, k int, n int, chunks []int) []byte {
	return nil
}
