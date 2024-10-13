package crypto_util

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
)

const (
	IV = "00112233445566778899aabbccddeeff"
)

// example
// manually encrypt the EH connection string
// openssl enc -aes-256-gcm-siv -in event-hub-constring-secret.txt -out ciphertext.bin -K 76dabc4cf1c8127220f1a401c658be5788f63df98e553f55aeeebc9acc78ad14 -iv 00112233445566778899aabb -p -nosalt
// openssl pkeyutl -encrypt -in event-hub-constring-secret.txt -out ciphertext.bin -keyform raw -aes-256-gcm -pkeyopt "key:76dabc4cf1c8127220f1a401c658be5788f63df98e553f55aeeebc9acc78ad14" -pkeyopt "iv:00112233445566778899aabb"
// openssl enc -aes-256-cbc -in event-hub-constring-secret.txt -out ciphertext.bin -K 76dabc4cf1c8127220f1a401c658be5788f63df98e553f55aeeebc9acc78ad14 -iv 00112233445566778899aabbccddeeff -p -nosalt
// Decrypt
// openssl pkeyutl -decrypt -in ciphertext.bin -out decrypted.txt -keyform raw -aes-256-gcm -pkeyopt "key:61207665727920766572792076657279207665727920736563726574206b6579" -pkeyopt "iv:00112233445566778899aabb"
// manually generate the key
//  echo -n "this is my test" | openssl dgst -sha256

// Give key
func GenerateKey(seed string) []byte {

	hash := sha256.New()
	hash.Write([]byte(seed))
	hashBytes := hash.Sum(nil)

	return hashBytes
}

// Encrypt function using AES-GCM
func EncryptAESGCM(plainText, key []byte) ([]byte, error) {
	// Create a new AES cipher block
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Join(err, errors.New("crypto util encrypt AES-GCM"))
	}

	// Use GCM mode
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Join(err, errors.New("crypto util encrypt AES-GCM"))
	}

	// Create a nonce. GCM standard recommends 12 bytes (96 bits).
	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, errors.Join(err, errors.New("crypto util encrypt AES-GCM"))
	}

	// Encrypt the data and append the nonce to the ciphertext
	cipherText := aesGCM.Seal(nonce, nonce, plainText, nil)

	// Return the encrypted data in base64 format
	//return base64.StdEncoding.EncodeToString(cipherText), nil
	return cipherText, nil
}

// Encrypt function using AES-CBC
func encryptAES256CBC(plaintext, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Join(err, errors.New("crypto util encrypt AES-CBC"))
	}

	if len(plaintext)%aes.BlockSize != 0 {
		// Ensure the plaintext is padded to a multiple of the block size
		paddingSize := aes.BlockSize - (len(plaintext) % aes.BlockSize)
		padding := make([]byte, paddingSize)
		plaintext = append(plaintext, padding...)
	}

	ciphertext := make([]byte, len(plaintext))
	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext, plaintext)

	return ciphertext, nil
}

// Decrypt function using AES-GCM
func DecryptAESGCM(cipherTextBase64 string, key []byte) ([]byte, error) {
	// Decode the base64 encoded ciphertext
	cipherText, err := base64.StdEncoding.DecodeString(cipherTextBase64)
	if err != nil {
		return nil, errors.Join(err, errors.New("crypto util decrypt AES-GCM"))
	}

	// Create a new AES cipher block
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Join(err, errors.New("crypto util decrypt AES-GCM"))
	}

	// Use GCM mode
	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.Join(err, errors.New("crypto util decrypt AES-GCM"))
	}

	// Split the nonce and the actual ciphertext
	nonceSize := aesGCM.NonceSize()
	nonce, cipherText := cipherText[:nonceSize], cipherText[nonceSize:]

	// Decrypt the data
	plainText, err := aesGCM.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return nil, errors.Join(err, errors.New("crypto util decrypt AES-GCM"))
	}

	return plainText, nil
}

// Decrypt function using AES-CBC
func DecryptAES256CBC(ciphertext, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.Join(err, errors.New("crypto util decrypt AES-GBC"))
	}

	plaintext := make([]byte, len(ciphertext))
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(plaintext, ciphertext)

	return plaintext, nil
}
