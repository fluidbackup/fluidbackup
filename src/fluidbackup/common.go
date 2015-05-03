package fluidbackup

import "math/rand"
import "crypto/sha1"
import "strconv"

/**
 * Contains mechanisms for server communication,
 * and shared definitions.
 */

const FILE_BLOCK_SIZE = 1024 * 1024
const DEFAULT_N = 12
const DEFAULT_K = 8

func randSeq(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    b := make([]rune, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

func hash(b []byte) []byte {
	hashArray := sha1.Sum(b)
	return hashArray[:]
}

func strToInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 0, 64)
	return n
}

func strToInt(s string) int {
	n, _ := strconv.ParseInt(s, 0, 32)
	return int(n)
}

func boolToInt(b bool) int {
	if b {
		return 1
	} else {
		return 0
	}
}
