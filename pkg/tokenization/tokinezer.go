package tokenization

import (
	"os"

	"github.com/daulet/tokenizers"
)

// Tokenizer interface defines the methods for tokenization.
type Tokenizer interface {
	// Encode tokenizes the input string and returns the token IDs and offsets.
	Encode(input, modelName string) ([]uint32, []tokenizers.Offset, error)
}

// HFTokenizer is a struct that implements the Tokenizer interface using
// bindings to HuggingFace's rust tokenizer.
type HFTokenizer struct {
	cfg tokenizers.TokenizerConfigOption
}

// NewHFTokenizer creates a new instance of HFTokenizer with the provided configuration.
func NewHFTokenizer() Tokenizer {
	cfg := tokenizers.WithAuthToken(os.Getenv("HF_TOKEN")) // Todo- use cache dir
	return &HFTokenizer{
		cfg: cfg,
	}
}

// Encode converts a string into token IDs.
func (t *HFTokenizer) Encode(input, modelName string) ([]uint32, []tokenizers.Offset, error) {
	tk, err := tokenizers.FromPretrained(modelName, t.cfg)
	if err != nil {
		return nil, nil, err
	}

	defer func(tk *tokenizers.Tokenizer) {
		err := tk.Close()
		if err != nil {
			return
		}
	}(tk)

	encodeOptions := []tokenizers.EncodeOption{
		tokenizers.WithReturnTypeIDs(),
		tokenizers.WithReturnOffsets(),
	}

	resp := tk.EncodeWithOptions(input, true, encodeOptions...)
	return resp.IDs, resp.Offsets, nil
}
