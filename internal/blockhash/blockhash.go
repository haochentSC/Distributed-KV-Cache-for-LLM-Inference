// Package blockhash computes the chained per-block cache keys (ADR 0011). It is
// kept separate from package cache so the server never depends on tokenization.
package blockhash

import (
	"crypto/sha256"
	"encoding/binary"
)

// seedPrefix domain-separates this project's hashes and is mixed with the model ID so
// two models never share a chain. Without the model in the seed, identical token
// prefixes on different models would collide on one key and evict each other (review
// "Bug 3"); folding model into the seed keeps the server purely opaque-keyed (ADR 0010)
// while making collisions impossible across models.
const seedPrefix = "kvcache/v1\x00"

// Block binds one full token block to the opaque hash derived from it.
type Block struct {
	Hash     [32]byte
	TokenIDs []int32
}

// Chain returns one 32-byte hash per FULL block of the token sequence:
//
//	seed          = SHA256( "kvcache/v1\x00" || modelID )
//	block_hash[0] = SHA256( seed          || encode(block 0) )
//	block_hash[i] = SHA256( block_hash[i-1] || encode(block i) )
//
// Token IDs are encoded little-endian int32. A trailing partial block (fewer than
// blockTokens tokens) is dropped — matching vLLM — so every hash covers exactly
// blockTokens tokens. Two sequences for the same model that share a prefix produce
// identical hashes until they diverge, which is what makes longest-prefix matching
// work. Returns nil if blockTokens <= 0 or there isn't even one full block.
func Chain(modelID string, tokenIDs []int32, blockTokens int) [][32]byte {
	blocks := ChainBlocks(modelID, tokenIDs, blockTokens)
	if blocks == nil {
		return nil
	}
	hashes := make([][32]byte, len(blocks))
	for i := range blocks {
		hashes[i] = blocks[i].Hash
	}
	return hashes
}

// ChainBlocks is Chain plus the exact token IDs covered by each hash. The token
// slices are copied, so callers can safely mutate the input token buffer afterward.
func ChainBlocks(modelID string, tokenIDs []int32, blockTokens int) []Block {
	if blockTokens <= 0 || len(tokenIDs) < blockTokens {
		return nil
	}
	numBlocks := len(tokenIDs) / blockTokens

	prev := sha256.Sum256([]byte(seedPrefix + modelID))
	blocks := make([]Block, 0, numBlocks)

	buf := make([]byte, sha256.Size+blockTokens*4) // reused each block: prev || tokens
	for b := 0; b < numBlocks; b++ {
		copy(buf[:sha256.Size], prev[:])
		off := sha256.Size
		toks := tokenIDs[b*blockTokens : (b+1)*blockTokens]
		for _, tok := range toks {
			binary.LittleEndian.PutUint32(buf[off:], uint32(tok))
			off += 4
		}
		prev = sha256.Sum256(buf)
		blockTokensCopy := append([]int32(nil), toks...)
		blocks = append(blocks, Block{Hash: prev, TokenIDs: blockTokensCopy})
	}
	return blocks
}
