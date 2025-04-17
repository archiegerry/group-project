package main

import (
	"bufio"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
)

/*
Cruncher tool for Reddit and News article processing.

  1. reddit-submissions:
      Converts Reddit submissions from a zstdcat JSONL stream to a Parquet file.
	 	 zstdcat wallstreetbets_submissions.zst | ./cruncher reddit-submissions output.parquet

  2. reddit-comments:
      Converts Reddit comments from a zstdcat JSONL stream to a Parquet file.
          zstdcat wallstreetbets_comments.zst | ./cruncher reddit-comments output.parquet

  3. news-articles:
      Converts GNews-style article data from a zstdcat JSONL stream to a Parquet file.
          zstdcat Accenture.jsonl.zst | ./cruncher news-articles output.parquet

  4. split-news:
      Splits a news Parquet file into multiple Parquet files by matched search terms.
          ./cruncher split-news input.parquet output_folder/

  5. split-reddit:
      Splits a Reddit Parquet file into multiple Parquet files by matched search terms.
          ./cruncher split-reddit input.parquet output_folder/

Notes:
- The splitter commands use a predefined search term mapping from:
	DataRetrieval/Stocks/data/search_terms_reduced.csv
*/


func main() {
	cmd := os.Args[1]

	// Reddit processes (submissions/comments)
	if cmd[:6] == "reddit" {
		// Next arg is output file
		outputFilename := os.Args[2]
		// file, err := os.Create(outputFilename)
		file, err := local.NewLocalFileWriter(outputFilename)
		if err != nil {
			panic("error creating output file: " + err.Error())
		}

		// Read json stream from stdin (use zstdcat |)
		scanner := bufio.NewScanner(os.Stdin)
		buf := make([]byte, 16*1024*1024)
		scanner.Buffer(buf, cap(buf))

		// Start process
		if cmd == "reddit-submissions" {
			err = ProcessRedditSubmissionsParquet(scanner, file, 10)
			if err != nil {
				panic("error processing reddit submissions: " + err.Error())
			}
		} else if cmd == "reddit-comments" {
			err = ProcessRedditCommentsParquet(scanner, file, 10)
			if err != nil {
				panic("error processing reddit comments: " + err.Error())
			}
		} else {
			panic("invalid reddit command: " + cmd + ", expected reddit-submissions or reddit-comments")
		}
	} else if cmd == "news-articles" {
		// Next arg is output file
		outputFilename := os.Args[2]
		// file, err := os.Create(outputFilename)
		file, err := local.NewLocalFileWriter(outputFilename)
		if err != nil {
			panic("error creating output file: " + err.Error())
		}

		// Read json stream from stdin (use zstdcat |)
		scanner := bufio.NewScanner(os.Stdin)
		buf := make([]byte, 16*1024*1024)
		scanner.Buffer(buf, cap(buf))

		// Start process
		err = ProcessNewsArticlesParquet(scanner, file, 10)
		if err != nil {
			panic("error processing news articles: " + err.Error())
		}

	} else if cmd == "split-news" {
		// Next arg is input file, followed by path (including trailing slash) of output
		parquetPath := os.Args[2]
		outputPath := os.Args[3]
		mapping := BuildSearchTermMapping("../../DataRetrieval/Stocks/data/search_terms_reduced.csv")
		SplitNewsParquet(parquetPath, outputPath, mapping)
	} else if cmd == "split-reddit" {
		// Next arg is input file, followed by path (including trailing slash) of output
		parquetPath := os.Args[2]
		outputPath := os.Args[3]
		mapping := BuildSearchTermMapping("../../DataRetrieval/Stocks/data/search_terms_reduced.csv")
		SplitRedditParquet(parquetPath, outputPath, mapping)
	} else {
		panic("invalid command: " + cmd + ", expected reddit-submissions, reddit-comments, or news-articles")
	}

}
