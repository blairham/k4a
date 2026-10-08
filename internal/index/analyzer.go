// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"bytes"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	"github.com/blevesearch/bleve/v2/registry"
)

// textAnalyzer is the analyzer every text field is indexed with: Bleve's
// standard analyzer (unicode word segmentation, lowercase, English stop
// words) with its lowercase filter replaced by lowercaseFilter.
const textAnalyzer = "k4a_standard"

// lowercaseFilterName is lowercaseFilter's registry name.
const lowercaseFilterName = "k4a_lowercase"

// lowercaseFilter lowercases each term with bytes.ToLower. Bleve's own filter
// (v2.6.0) corrupts a term once a rune lowercases to a shorter encoding --
// İ, the Kelvin sign K, the Ångström sign Å, the Ohm sign Ω: the bytes after
// it are left unshifted, so "İstanbul" is indexed as "i\xb0stanbu" and no
// search for anything in that term can find it.
type lowercaseFilter struct{}

func (lowercaseFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for _, token := range input {
		token.Term = bytes.ToLower(token.Term)
	}
	return input
}

func init() {
	err := registry.RegisterTokenFilter(lowercaseFilterName,
		func(map[string]any, *registry.Cache) (analysis.TokenFilter, error) {
			return lowercaseFilter{}, nil
		})
	if err != nil {
		panic(err)
	}
}

// textAnalyzerConfig defines textAnalyzer for a Bleve index mapping.
func textAnalyzerConfig() map[string]any {
	return map[string]any{
		"type":          custom.Name,
		"tokenizer":     unicode.Name,
		"token_filters": []any{lowercaseFilterName, en.StopName},
	}
}
