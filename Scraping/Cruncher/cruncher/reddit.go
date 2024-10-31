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
)

type RedditSubmissionRaw struct {
	Title      string      `json:"title"`
	CreatedUTC json.Number `json:"created_utc"`
	Subreddit  string      `json:"subreddit"`
	Domain     string      `json:"domain"`
	Score      int32       `json:"score"`
	PostId     string      `json:"id"`
}

type RedditCommentRaw struct {
	Body       string      `json:"body"`
	CreatedUTC json.Number `json:"created_utc"`
	Score      int32       `json:"score"`
	ParentId   string      `json:"parent_id"`
}

type RedditSubmission struct {
	Title     string `parquet:"name=title, type=BYTE_ARRAY, convertedtype=UTF8"`
	Datetime  int64  `parquet:"name=dt, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	Subreddit string `parquet:"name=subreddit, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Domain    string `parquet:"name=domain, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Score     int32  `parquet:"name=score, type=INT32, convertedtype=UTF8"`
	PostId    string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type RedditComment struct {
	Body     string `parquet:"name=body, type=BYTE_ARRAY, convertedtype=UTF8"`
	Datetime int64  `parquet:"name=dt, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	Score    int32  `parquet:"name=score, type=INT32, convertedtype=UTF8"`
	ParentId string `parquet:"name=parent_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func (s *RedditSubmissionRaw) ToSubmission() RedditSubmission {
	timestamp, err := s.CreatedUTC.Int64()
	if err != nil {
		str := s.CreatedUTC.String()
		timestamp2, err := strconv.Atoi(strings.Split(str, ".")[0])
		if err != nil {
			panic("error parsing time int: " + err.Error())
		}
		timestamp = int64(timestamp2)
	}
	return RedditSubmission{
		Title:     s.Title,
		Datetime:  int64(1000 * timestamp),
		Subreddit: s.Subreddit,
		Domain:    s.Domain,
		Score:     s.Score,
		PostId:    s.PostId,
	}
}

func (s *RedditCommentRaw) ToComment() RedditComment {
	timestamp, err := s.CreatedUTC.Int64()
	if err != nil {
		str := s.CreatedUTC.String()
		timestamp2, err := strconv.Atoi(strings.Split(str, ".")[0])
		if err != nil {
			panic("error parsing time int: " + err.Error())
		}
		timestamp = int64(timestamp2)
	}
	return RedditComment{
		Body:     s.Body,
		Datetime: int64(1000 * timestamp),
		Score:    s.Score,
		ParentId: s.ParentId,
	}
}

// Read input json stream and output to csv file (can later be parquet)
func ProcessRedditSubmissions(scanner *bufio.Scanner, output *os.File) error {
	// Write column names
	writer := csv.NewWriter(output)
	writer.Write([]string{"title", "dt", "subreddit", "domain", "score", "id"})

	// Loop through each line from stdin
	var err error
	var submissionRaw RedditSubmissionRaw
	i := 0
	for scanner.Scan() {
		if i%10000 == 0 {
			log.Println(i)
		}
		i += 1
		line := scanner.Bytes()
		submissionRaw = RedditSubmissionRaw{}
		err = json.Unmarshal(line, &submissionRaw)
		if err != nil {
			return fmt.Errorf("error parsing line: " + string(line) + ", " + err.Error())
		}
		submission := submissionRaw.ToSubmission()
		// writer.Write(submission)
		writer.Write([]string{submission.Title, strconv.FormatInt(submission.Datetime, 10), submission.Subreddit, submission.Domain, strconv.Itoa(int(submission.Score)), submission.PostId})
	}
	writer.Flush()
	output.Close()

	// Check for errors that may have occurred during scanning
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading from stdin: " + err.Error())
	}
	log.Println("Done.")
	return nil
}

// Read input json stream and output to csv file (can later be parquet)
func ProcessRedditComments(scanner *bufio.Scanner, output *os.File) error {
	// Write column names
	writer := csv.NewWriter(output)
	writer.Write([]string{"body", "dt", "score", "parent_id"})

	// Loop through each line from stdin
	var err error
	var commentRaw RedditCommentRaw
	i := 0
	for scanner.Scan() {
		if i%10000 == 0 {
			log.Println(i)
		}
		i += 1
		line := scanner.Bytes()
		commentRaw = RedditCommentRaw{}
		err = json.Unmarshal(line, &commentRaw)
		if err != nil {
			return fmt.Errorf("error parsing line: " + string(line) + ", " + err.Error())
		}
		comment := commentRaw.ToComment()
		writer.Write([]string{comment.Body, strconv.FormatInt(comment.Datetime, 10), strconv.Itoa(int(comment.Score)), comment.ParentId})
	}
	writer.Flush()
	output.Close()

	// Check for errors that may have occurred during scanning
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading from stdin: " + err.Error())
	}
	log.Println("Done.")
	return nil
}
