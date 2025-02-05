package main

import (
	"bufio"
	"os"

	"github.com/xitongsys/parquet-go-source/local"
)

/*

Script to convert reddit jsonl streams into a csv file for processing.

To use:
zstdcat wallstreetbets_submissions.zst | ./cruncher reddit-submissions output.csv
zstdcat wallstreetbets_comments.zst | ./cruncher reddit-comments output.parquet

OR 

zstdcat Accenture.jsonl.zst | ./cruncher news-articles output.parqet

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

	} else {
		panic("invalid command: " + cmd + ", expected reddit-submissions, reddit-comments, or news-articles")
	}

}
