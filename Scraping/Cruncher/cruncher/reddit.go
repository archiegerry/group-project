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

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"

	// "github.com/apache/arrow/go/parquet/pqarrow"

	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type RedditSubmissionRaw struct {
	Title      string      `json:"title"`
	CreatedUTC json.Number `json:"created_utc"`
	Subreddit  string      `json:"subreddit"`
	Domain     string      `json:"domain"`
	Score      int64       `json:"score"`
	Downs      int64       `json:"downs"`
	PostId     string      `json:"id"`
	Body       string      `json:"selftext"`
	Flair      string      `json:"link_flair_text"`
}

type RedditCommentRaw struct {
	Body       string      `json:"body"`
	CreatedUTC json.Number `json:"created_utc"`
	Score      int64       `json:"score"`

	Id string `json:"id"`
	// Link ID starts with t3_..., the ... is ID of the submission
	LinkId string `json:"link_id"`
	// If it's a string, use parent_id.split("_")[-1], otherwise use permalink.split("/")[4]
	ParentId  interface{} `json:"parent_id"`
	Permalink string      `json:"permalink"`
}

type RedditSubmission struct {
	Title     string `parquet:"name=title, type=BYTE_ARRAY, convertedtype=UTF8"`
	Subreddit string `parquet:"name=subreddit, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Domain    string `parquet:"name=domain, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PostId    string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Datetime  int64  `parquet:"name=dt, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	Score     int64  `parquet:"name=score, type=INT32, convertedtype=UTF8"`
	Downs     int64  `parquet:"name=downs, type=INT32, convertedtype=UTF8"`
	Body      string `parquet:"name=body, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Flair     string `parquet:"name=Flair, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type RedditComment struct {
	Body     string `parquet:"name=body, type=BYTE_ARRAY, convertedtype=UTF8"`
	Id       string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PostId   string `parquet:"name=post_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ParentId string `parquet:"name=parent_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Datetime int64  `parquet:"name=dt, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	Score    int64  `parquet:"name=score, type=INT32, convertedtype=UTF8"`
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
		Downs:     s.Downs,
		PostId:    s.PostId,
		Body:      s.Body,
		Flair:     s.Flair,
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
	parentId := ""
	permalinkParts := strings.Split(parentId, "/")
	if len(permalinkParts) > 4 {
		parentId = permalinkParts[4]
	} else {
		switch v := s.ParentId.(type) {
		case string:
			parentId = strings.Split(v, "_")[1]
		}
	}
	return RedditComment{
		Body:     s.Body,
		Datetime: int64(1000 * timestamp),
		Score:    s.Score,
		Id:       s.Id,
		PostId:   strings.Replace(s.LinkId, "t3_", "", 1),
		ParentId: parentId,
	}
}

// Read input json stream and output to csv file (can later be parquet)
func ProcessRedditSubmissions(scanner *bufio.Scanner, output *os.File) error {
	// Write column names
	writer := csv.NewWriter(output)
	writer.Write([]string{"title", "dt", "subreddit", "domain", "score", "id", "body"})

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
		writer.Write([]string{submission.Title, strconv.FormatInt(submission.Datetime, 10), submission.Subreddit, submission.Domain, strconv.Itoa(int(submission.Score)), strconv.Itoa(int(submission.Downs)), submission.PostId, submission.Body, submission.Flair})
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

// Read input json stream and output to parquet file, have to manually write columns
func ProcessRedditSubmissionsParquet(scanner *bufio.Scanner, output source.ParquetFile, minScore int64) error {
	// Create parquet writer
	// Initialize column builders
	pool := memory.NewGoAllocator()
	stringBuilders := make([]*array.StringBuilder, 6)
	for i := range stringBuilders {
		stringBuilders[i] = array.NewStringBuilder(pool)
	}
	intBuilders := make([]*array.Int64Builder, 3)
	for i := range intBuilders {
		intBuilders[i] = array.NewInt64Builder(pool)
	}

	// Define the schema for Arrow
	fields := []arrow.Field{
		{Name: "title", Type: arrow.BinaryTypes.String},
		{Name: "subreddit", Type: arrow.BinaryTypes.String},
		{Name: "domain", Type: arrow.BinaryTypes.String},
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "body", Type: arrow.BinaryTypes.String},
		{Name: "flair", Type: arrow.BinaryTypes.String},
		// Epoch milliseconds
		{Name: "dt", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Int64},
		{Name: "downs", Type: arrow.PrimitiveTypes.Int64},
	}
	schema := arrow.NewSchema(fields, nil)

	// Initialize the Parquet writer
	arrowWriter, err := writer.NewArrowWriter(schema, output, 1)
	if err != nil {
		return err
	}

	currentRowCount := int64(0)
	// Flush to file
	flush := func() {
		// Convert builders to Arrow arrays
		columns := make([]array.Interface, len(stringBuilders)+len(intBuilders))
		for i, builder := range stringBuilders {
			columns[i] = builder.NewArray()
		}
		for i, builder := range intBuilders {
			columns[i+len(stringBuilders)] = builder.NewArray()
		}

		// Create a new record batch from the arrays
		record := array.NewRecord(schema, columns, currentRowCount)
		defer record.Release()

		// Write the record batch to the Parquet file
		if err := arrowWriter.WriteArrow(record); err != nil {
			log.Fatal(err)
		}
	}

	// Loop through each line from stdin
	var submissionRaw RedditSubmissionRaw
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		submissionRaw = RedditSubmissionRaw{}
		err = json.Unmarshal(line, &submissionRaw)
		if err != nil {
			return fmt.Errorf("error parsing line: " + string(line) + ", " + err.Error())
		}
		if submissionRaw.Score < minScore {
			continue
		}
		s := submissionRaw.ToSubmission()
		// Add to builders
		stringBuilders[0].Append(s.Title)
		stringBuilders[1].Append(s.Subreddit)
		stringBuilders[2].Append(s.Domain)
		stringBuilders[3].Append(s.PostId)
		stringBuilders[4].Append(s.Body)
		stringBuilders[5].Append(s.Flair)
		intBuilders[0].Append(s.Datetime)
		intBuilders[1].Append(s.Score)
		intBuilders[2].Append(s.Downs)

		// Flush at interval
		i += 1
		if i%4096 == 0 {
			log.Println(i)
			flush()
		}

	}
	flush()
	arrowWriter.WriteStop()
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
	writer.Write([]string{"body", "dt", "score", "id", "post_id", "parent_id"})

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

// Read input json stream and output to parquet file, have to manually write columns
func ProcessRedditCommentsParquet(scanner *bufio.Scanner, output source.ParquetFile, minScore int64) error {
	// Create parquet writer
	// Initialize column builders
	pool := memory.NewGoAllocator()
	stringBuilders := make([]*array.StringBuilder, 4)
	for i := range stringBuilders {
		stringBuilders[i] = array.NewStringBuilder(pool)
	}
	intBuilders := make([]*array.Int64Builder, 2)
	for i := range intBuilders {
		intBuilders[i] = array.NewInt64Builder(pool)
	}

	// Define the schema for Arrow
	fields := []arrow.Field{
		{Name: "body", Type: arrow.BinaryTypes.String},
		{Name: "id", Type: arrow.BinaryTypes.String},
		{Name: "post_id", Type: arrow.BinaryTypes.String},
		{Name: "parent_id", Type: arrow.BinaryTypes.String},
		// Epoch milliseconds
		{Name: "dt", Type: arrow.PrimitiveTypes.Int64},
		{Name: "score", Type: arrow.PrimitiveTypes.Int64},
	}
	schema := arrow.NewSchema(fields, nil)

	// Initialize the Parquet writer
	arrowWriter, err := writer.NewArrowWriter(schema, output, 1)
	if err != nil {
		return err
	}

	currentRowCount := int64(0)
	// Flush to file
	flush := func() {
		// Convert builders to Arrow arrays
		columns := make([]array.Interface, len(stringBuilders)+len(intBuilders))
		for i, builder := range stringBuilders {
			columns[i] = builder.NewArray()
		}
		for i, builder := range intBuilders {
			columns[i+len(stringBuilders)] = builder.NewArray()
		}

		// Create a new record batch from the arrays
		record := array.NewRecord(schema, columns, currentRowCount)
		defer record.Release()

		// Write the record batch to the Parquet file
		if err := arrowWriter.WriteArrow(record); err != nil {
			log.Fatal(err)
		}
	}

	// Loop through each line from stdin
	var commentRaw RedditCommentRaw
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		commentRaw = RedditCommentRaw{}
		err = json.Unmarshal(line, &commentRaw)
		if err != nil {
			return fmt.Errorf("error parsing line: " + string(line) + ", " + err.Error())
		}
		if commentRaw.Score < minScore {
			continue
		}
		c := commentRaw.ToComment()
		// Add to builders
		stringBuilders[0].Append(c.Body)
		stringBuilders[1].Append(c.Id)
		stringBuilders[2].Append(c.PostId)
		stringBuilders[3].Append(c.ParentId)
		intBuilders[0].Append(c.Datetime)
		intBuilders[1].Append(c.Score)

		// Flush at interval
		i += 1
		if i%4096 == 0 {
			log.Println(i)
			flush()
		}

	}
	flush()
	arrowWriter.WriteStop()
	output.Close()

	// Check for errors that may have occurred during scanning
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading from stdin: " + err.Error())
	}
	log.Println("Done.")
	return nil
}
