package fluidbackup

import "crypto/aes"
import "crypto/cipher"
import "crypto/rand"

func cryptGenerateKey() ([]byte, error) {
	b := make([]byte, 32 + aes.BlockSize)
	_, err := rand.Read(b)
	return b, err
}

func cryptEncrypt(key []byte, original []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:32])
	if err != nil {
		return nil, err
	}
	ciphertext := make([]byte, len(original))
	cfb := cipher.NewCFBEncrypter(block, key[32:])
	cfb.XORKeyStream(ciphertext, original)
	return ciphertext, nil
}

func cryptDecrypt(key []byte, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:32])
	if err != nil {
		return nil, err
	}
	cfb := cipher.NewCFBDecrypter(block, key[32:])
	cfb.XORKeyStream(ciphertext, ciphertext)
	return ciphertext, nil
}
