package fluidbackup

import "math/rand"
import "crypto/sha1"

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
