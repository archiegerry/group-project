package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	// "github.com/apache/arrow/go/parquet/pqarrow"
)

/*

Script to convert reddit jsonl streams into a csv file for processing.

TODO - move to parquet.

*/

type SubmissionRaw struct {
	Title      string      `json:"title"`
	CreatedUTC json.Number `json:"created_utc"`
	Subreddit  string      `json:"subreddit"`
	Domain     string      `json:"domain"`
	Score      int32       `json:"score"`
	PostId     string      `json:"id"`
}

type Submission struct {
	Title     string `parquet:"name=title, type=BYTE_ARRAY, convertedtype=UTF8"`
	Datetime  int64  `parquet:"name=dt, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	Subreddit string `parquet:"name=subreddit, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Domain    string `parquet:"name=domain, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Score     int32  `parquet:"name=score, type=INT32, convertedtype=UTF8"`
	PostId    string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func (s *SubmissionRaw) ToSubmission() Submission {
	timestamp, err := s.CreatedUTC.Int64()
	if err != nil {
		str := s.CreatedUTC.String()
		timestamp2, err := strconv.Atoi(strings.Split(str, ".")[0])
		if err != nil {
			panic("error parsing time int: " + err.Error())
		}
		timestamp = int64(timestamp2)
	}
	return Submission{
		Title:     s.Title,
		Datetime:  int64(1000 * timestamp),
		Subreddit: s.Subreddit,
		Domain:    s.Domain,
		Score:     s.Score,
		PostId:    s.PostId,
	}
}

func main() {
	outfile := os.Args[1]
	file, err := os.Create(outfile)
	if err != nil {
		panic(err)
	}

	// Parquet writer
	writer := csv.NewWriter(file)
	writer.Write([]string{"title", "dt", "subreddit", "domain", "score", "id"})
	// writer, err := writer.NewParquetWriterFromWriter(file, new(Submission), 4)
	// if err != nil {
	// 	panic(err)
	// }
	// writer.RowGroupSize = 128 * 1024 * 1024 //128M
	// writer.CompressionType = parquet.CompressionCodec_SNAPPY

	// Read input
	scanner := bufio.NewScanner(os.Stdin)
	buf := make([]byte, 16*1024*1024)
	scanner.Buffer(buf, cap(buf))

	// Loop through each line from stdin
	var submissionRaw SubmissionRaw
	i := 0
	for scanner.Scan() {
		if i%10000 == 0 {
			log.Println(i)
		}
		i += 1
		line := scanner.Bytes()
		submissionRaw = SubmissionRaw{}
		err = json.Unmarshal(line, &submissionRaw)
		if err != nil {
			panic("error parsing line: " + string(line) + ", " + err.Error())
		}
		submission := submissionRaw.ToSubmission()
		// writer.Write(submission)
		writer.Write([]string{submission.Title, strconv.FormatInt(submission.Datetime, 10), submission.Subreddit, submission.Domain, strconv.Itoa(int(submission.Score)), submission.PostId})
	}

	// if err = writer.WriteStop(); err != nil {
	// 	log.Println("WriteStop error", err)
	// 	return
	// }
	writer.Flush()
	file.Close()

	// Check for errors that may have occurred during scanning
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "error reading from stdin:", err)
		os.Exit(1)
	}
	log.Println("Done.")
}
