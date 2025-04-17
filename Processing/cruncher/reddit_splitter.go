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


// Read parquet, split articles, maintain file pointers
func SplitRedditParquet(submissionsPath, outputPath string, mapping SearchTermNode) error {
	commentsPath := strings.Replace(submissionsPath, "submissions", "comments", 1)
	outputCommentsPath := strings.Replace(outputPath, "submissions", "comments", 1)
	// Start with submissions, build map of post_id -> ticker, then do comments
	fr, err := local.NewLocalFileReader(submissionsPath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	pr, err := reader.NewParquetReader(fr, new(RedditSubmission), 4)
	if err != nil {
		return fmt.Errorf("error creating parquet reader: %v", err)
	}

	// Map of reddit_id -> ticker
	// Includes both comment and post ids
	mapToTicker := map[string]string{}
	allPosts := map[string]RedditSubmission{}

	// File map
	files := map[string]*os.File{}
	csvWriters := map[string]*csv.Writer{}

	numRows := int(pr.GetNumRows())
	batchSize := 1000
	posts := make([]RedditSubmission, batchSize)
	for i := 0; i <= numRows/batchSize; i++ {
		if err = pr.Read(&posts); err != nil {
			log.Printf("Error reading row %d: %v", i, err)
			continue
		}
		for _, post := range posts {
			allPosts[post.PostId] = post

			// Convert timestamp to readable time
			tagged := TagText(post.Title+"\n"+string(post.Body), "", mapping)
			tickerRegionSize := map[string]int{}
			for _, t := range tagged {
				// No company mentioned - don't output
				if t.Ticker == "" {
					continue
				}

				if _, ok := csvWriters[t.Ticker]; !ok {
					// Add artifact writer
					f, err := os.OpenFile(outputPath+t.Ticker+".csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
					if err != nil {
						panic(err)
					}
					files[t.Ticker] = f
					csvWriters[t.Ticker] = csv.NewWriter(f)
				}
				writer := csvWriters[t.Ticker]

				// Columns: post_id,text,domain,flair,subreddit,score,downs,datetime
				writer.Write([]string{
					post.PostId,
					t.Text,
					// meta
					post.Domain,
					post.Flair,
					post.Subreddit,
					strconv.FormatInt(post.Score, 10),
					strconv.FormatInt(post.Downs, 10),
					strconv.FormatInt(post.Datetime, 10),
				})

				// Update ticker region size
				tickerRegionSize[t.Ticker] += len(t.Text)
			}

			// Update mapToTicker
			maxSize := 0
			maxTicker := ""
			for ticker, size := range tickerRegionSize {
				if size > maxSize {
					maxSize = size
					maxTicker = ticker
				}
			}
			mapToTicker[post.PostId] = maxTicker
		}
	}

	for ticker, file := range files {
		csvWriters[ticker].Flush()
		file.Close()
	}
	fr.Close()
	pr.ReadStop()

	// New files and writers
	files = map[string]*os.File{}
	csvWriters = map[string]*csv.Writer{}

	// Now comments, we need to sort by datetime and construct a map of comment_id -> ticker to ensure
	// child comments can see ticker of parent comment
	fr, err = local.NewLocalFileReader(commentsPath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}

	pr, err = reader.NewParquetReader(fr, new(RedditComment), 4)
	if err != nil {
		return fmt.Errorf("error creating parquet reader: %v", err)
	}

	// Comments need to be sorted by datetime ascending
	numRows = int(pr.GetNumRows())
	batchSize = 1000
	comments := []RedditComment{}
	commentsBatch := make([]RedditComment, batchSize)
	for i := 0; i <= numRows/batchSize; i++ {
		if err = pr.Read(&commentsBatch); err != nil {
			log.Printf("Error reading row %d: %v", i, err)
			continue
		}
		comments = append(comments, commentsBatch...)
	}

	// Sort comments by datetime
	log.Println("sorting", len(comments), "comments")
	sort.Slice(comments, func(i, j int) bool {
		return comments[i].Datetime < comments[j].Datetime
	})

	matches := 0
	for _, comment := range comments {
		startTicker := mapToTicker[comment.ParentId]
		post := allPosts[comment.PostId]
		if startTicker != "" {
			matches += 1
		}
		// log.Println(startTicker, comment.ParentId)
		// panic("")

		// Convert timestamp to readable time
		tagged := TagText(comment.Body, startTicker, mapping)
		tickerRegionSize := map[string]int{}
		for _, t := range tagged {
			// No company mentioned - don't output
			if t.Ticker == "" {
				continue
			}

			if _, ok := csvWriters[t.Ticker]; !ok {
				// Add artifact writer
				f, err := os.OpenFile(outputCommentsPath+t.Ticker+".csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
				if err != nil {
					panic(err)
				}
				files[t.Ticker] = f
				csvWriters[t.Ticker] = csv.NewWriter(f)
			}
			writer := csvWriters[t.Ticker]

			// Columns: comment_id,text,score,datetime,parent_id,start_ticker,post_id,flair,subreddit,post_score,post_downs,post_datetime
			writer.Write([]string{
				comment.Id,
				t.Text,
				// comment meta
				strconv.FormatInt(comment.Score, 10),
				strconv.FormatInt(comment.Datetime, 10),
				comment.ParentId,
				// post meta
				startTicker,
				post.PostId,
				post.Flair,
				post.Subreddit,
				strconv.FormatInt(post.Score, 10),
				strconv.FormatInt(post.Downs, 10),
				strconv.FormatInt(post.Datetime, 10),
			})

			// Update ticker region size
			tickerRegionSize[t.Ticker] += len(t.Text)
		}

		// Update mapToTicker
		maxSize := 0
		maxTicker := ""
		for ticker, size := range tickerRegionSize {
			if size > maxSize {
				maxSize = size
				maxTicker = ticker
			}
		}
		mapToTicker[comment.Id] = maxTicker
	}
	log.Println(matches, "matches of", len(comments), " comments")
	for ticker, file := range files {
		csvWriters[ticker].Flush()
		file.Close()
	}
	fr.Close()
	pr.ReadStop()

	return nil
}
