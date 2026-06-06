package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// traceRecord is one replayable request: the full token context at a conversation turn,
// tokenized offline by scripts/prep_sharegpt.py with a REAL tokenizer so block lengths and
// boundaries match a real model. We carry only the token IDs — the cache derives block
// hashes from them (internal/blockhash, ADR 0010), and payloads stay synthetic. Conv/Turn
// are kept for ordering/debugging and possible per-conversation reporting later.
type traceRecord struct {
	Conv   int     `json:"conv"`
	Turn   int     `json:"turn"`
	Tokens []int32 `json:"tokens"`
}

// loadTrace reads a JSONL trace (one traceRecord per line) produced by prep_sharegpt.py.
// Records are kept in file order: the prep script emits each conversation's turns in
// ascending order, so replaying in order means turn N is written before turn N+1 fetches
// it — which is what produces realistic multi-turn prefix reuse.
func loadTrace(path string) ([]traceRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open trace: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	// Token arrays make for long lines; raise the line cap well above the default 64 KiB.
	sc.Buffer(make([]byte, 1<<20), 64<<20)

	var recs []traceRecord
	line := 0
	for sc.Scan() {
		line++
		b := sc.Bytes()
		if len(b) == 0 {
			continue
		}
		var r traceRecord
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("trace line %d: %w", line, err)
		}
		if len(r.Tokens) > 0 {
			recs = append(recs, r)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read trace: %w", err)
	}
	return recs, nil
}
