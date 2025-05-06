package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

/*

Logic for splitting a processed gnews parquet file into text artifacts and meta JSONL files.
We read one file at a time and stream to ~500 ticker files (mapped by the reduced search term CSV) simultaneously.


*/

type SearchTermNode struct {
	// If an end node, ticker will be nonempty
	Ticker string
	// Otherwise Map will be populated
	Map map[rune]SearchTermNode
}

/*
Construct a tree of each
*/
func BuildSearchTermMapping(path string) SearchTermNode {
	// Read search term CSV
	f, err := os.Open(path)
	if err != nil {
		log.Fatal("Unable to read search term file "+path, err)
	}
	defer f.Close()
	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+path, err)
	}

	// Construct base search node
	baseNode := SearchTermNode{
		Map: map[rune]SearchTermNode{},
	}

	// Skip header
	for _, record := range records[1:] {
		ticker, termsRaw := record[0], record[1]
		terms := strings.Split(termsRaw, "|")
		sort.Slice(terms, func(i, j int) bool {
			return len(terms[i]) < len(terms[j])
		})
		for _, term := range terms {
			// Each rune (character) in the term
			node := baseNode
			for i, c := range []rune(term) {
				ex, ok := node.Map[c]
				if i == len(term)-1 {
					// Final character - add ticker
					if ok {
						ex.Ticker = ticker
						node.Map[c] = ex
					} else {
						node.Map[c] = SearchTermNode{Ticker: ticker, Map: map[rune]SearchTermNode{}}
					}
				} else {
					// Other characters - add to map
					if ok {
						node = ex
					} else {
						node.Map[c] = SearchTermNode{Map: map[rune]SearchTermNode{}}
						node = node.Map[c]
					}
				}
			}
		}
	}
	return baseNode
}

const (
	// Chars that must precede a valid ticker name
	StartPhraseChars = " \t\n\"("
	// Chars that must follow a valid ticker name
	EndPhraseChars = " \t\n\"):?!."
)

type TaggedText struct {
	Ticker, Text string
}

func TagText(text, startTicker string, mapping SearchTermNode) []TaggedText {
	output := []TaggedText{}

	// State
	working := ""
	currentTicker := startTicker

	// Only match following specific chars - ' ', '"', '('
	lastCharWasValidStart := true

	// Local search
	node := mapping
	ok := false
	j := 0
	textRunes := []rune(text)
	for i, c := range textRunes {
		// See if we've found a search term, not most efficient
		if lastCharWasValidStart {
			node, ok = mapping.Map[c]
			j = i
			for j < len(textRunes) && ok {
				// Termination at end of string or termination character
				if j == len(textRunes)-1 || strings.ContainsRune(EndPhraseChars, textRunes[j+1]) {
					// Next is a valid end, check if we've reached an ending node
					if ok && node.Ticker != "" {
						if currentTicker == "" {
							currentTicker = node.Ticker
						}
						// log.Println(finalNode, string(textRunes[i:j+1]), currentTicker)
						// Only append if ticker has changed
						if currentTicker != node.Ticker {
							output = append(output, TaggedText{Ticker: currentTicker, Text: working})
							currentTicker = node.Ticker
							working = ""
						}
					}
				}
				j++
				if j < len(textRunes) {
					node, ok = node.Map[textRunes[j]]
				}
			}
		}
		working = working + string(c)

		// ready for next row
		lastCharWasValidStart = strings.ContainsRune(StartPhraseChars, c)
		node = mapping
	}

	// Any leftover text
	if len(working) > 0 {
		output = append(output, TaggedText{
			Ticker: currentTicker,
			Text:   working,
		})
	}
	return output
}

// Read parquet, split articles, maintain file pointers
func SplitNewsParquet(path, gnewsPath string, mapping SearchTermNode) error {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer fr.Close()
	pr, err := reader.NewParquetReader(fr, new(NewsArticle), 4)
	if err != nil {
		return fmt.Errorf("error creating parquet reader: %v", err)
	}
	defer pr.ReadStop()

	// File map
	files := map[string]*os.File{}
	csvWriters := map[string]*csv.Writer{}

	numRows := int(pr.GetNumRows())
	batchSize := 1000
	articles := make([]NewsArticle, batchSize)
	for i := 0; i <= numRows/batchSize; i++ {
		if err = pr.Read(&articles); err != nil {
			log.Printf("Error reading row %d: %v", i, err)
			continue
		}
		for _, article := range articles {
			// Convert timestamp to readable time
			// timestamp := time.Unix(0, article.DateTime*int64(time.Millisecond))
			tagged := TagText(article.Title+"\n"+string(article.Body), "", mapping)
			for _, t := range tagged {
				if t.Ticker == "" {
					continue
				}
				if _, ok := csvWriters[t.Ticker]; !ok {
					// Add artifact writer
					f, err := os.OpenFile(gnewsPath+t.Ticker+".csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
					if err != nil {
						panic(err)
					}
					files[t.Ticker] = f
					csvWriters[t.Ticker] = csv.NewWriter(f)
				}
				writer := csvWriters[t.Ticker]

				writer.Write([]string{
					article.Url,
					t.Text,
					// meta
					article.Domain,
					strconv.FormatInt(article.DateTime, 10),
				})
			}
		}
	}

	for ticker, file := range files {
		csvWriters[ticker].Flush()
		file.Close()
	}
	return nil
}
