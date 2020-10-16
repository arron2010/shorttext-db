package utils

import (
	"reflect"
	"unsafe"
)

const (
	c1 = 0xcc9e2d51
	c2 = 0x1b873593
	c3 = 0x85ebca6b
	c4 = 0xc2b2ae35
	nh = 0xe6546b64
)

var (
	// defaultSeed default murmur seed
	defaultSeed = uint32(1)
)

func LeveldbHash(b []byte) uint32 {

	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)

	h := uint32(seed) ^ uint32(len(b)*m)

	for ; len(b) >= 4; b = b[4:] {

		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}

	return h
}

// Murmur3 returns a hash from the provided key using the specified seed.
func Hash32(key []byte, seed ...uint32) uint32 {
	return Hash32ForStr(
		*(*string)((unsafe.Pointer)(
			&reflect.StringHeader{
				Len:  len(key),
				Data: (*reflect.SliceHeader)(unsafe.Pointer(&key)).Data,
			})),
		seed...)

	// return Sum32(string(key), seed...)
}

// Sum32 returns a hash from the provided key.
func Hash32ForStr(key string, seed ...uint32) (hash uint32) {
	hash = defaultSeed
	if len(seed) > 0 {
		hash = seed[0]
	}

	iByte := 0
	for ; iByte+4 <= len(key); iByte += 4 {
		k := uint32(key[iByte]) | uint32(key[iByte+1])<<8 |
			uint32(key[iByte+2])<<16 | uint32(key[iByte+3])<<24

		k *= c1
		k = (k << 15) | (k >> 17)
		k *= c2
		hash ^= k

		hash = (hash << 13) | (hash >> 19)
		hash = hash*5 + nh
	}

	var remainingBytes uint32
	switch len(key) - iByte {
	case 3:
		remainingBytes += uint32(key[iByte+2]) << 16
		fallthrough
	case 2:
		remainingBytes += uint32(key[iByte+1]) << 8
		fallthrough
	case 1:
		remainingBytes += uint32(key[iByte])
		remainingBytes *= c1

		remainingBytes = (remainingBytes << 15) | (remainingBytes >> 17)
		remainingBytes = remainingBytes * c2
		hash ^= remainingBytes
	}

	hash ^= uint32(len(key))
	hash ^= hash >> 16
	hash *= c3
	hash ^= hash >> 13
	hash *= c4
	hash ^= hash >> 16

	return
}
