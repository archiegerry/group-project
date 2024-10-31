package main

import (
	"bufio"
	"os"
)

/*

Script to convert reddit jsonl streams into a csv file for processing.

To use:
zstdcat wallstreetbets_submissions.zst | ./cruncher reddit-submissions output.csv
zstdcat wallstreetbets_comments.zst | ./cruncher reddit-comments output.csv

TODO - write to parquet instead of csv.

*/

func main() {
	cmd := os.Args[1]

	// Reddit processes (submissions/comments)
	if cmd[:6] == "reddit" {
		// Next arg is output file
		outputFilename := os.Args[1]
		file, err := os.Create(outputFilename)
		if err != nil {
			panic("error creating output file: " + err.Error())
		}

		// Read json stream from stdin (use zstdcat |)
		scanner := bufio.NewScanner(os.Stdin)
		buf := make([]byte, 16*1024*1024)
		scanner.Buffer(buf, cap(buf))

		// Start process
		if cmd == "reddit-submissions" {
			err = ProcessRedditSubmissions(scanner, file)
			if err != nil {
				panic("error processing reddit submissions: " + err.Error())
			}
		} else if cmd == "reddit-comments" {
			err = ProcessRedditComments(scanner, file)
			if err != nil {
				panic("error processing reddit comments: " + err.Error())
			}
		} else {
			panic("invalid reddit command: " + cmd + ", expected reddit-submissions or reddit-comments")
		}
	} else {
		panic("invalid command: " + cmd + ", expected reddit-submissions or reddit-comments")
	}

}
