package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"

	// "github.com/apache/arrow/go/parquet/pqarrow"

	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type NewsArticleRaw struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Body        string `json:"content"`
	Url         string `json:"url"`
	DateTime    string `json:"publishedAt"`
	Domain      struct {
		Name string `json:"name"`
	} `json:"source"`
}

type NewsArticle struct {
	Title       string `parquet:"name=title, type=BYTE_ARRAY, convertedtype=UTF8"`
	Description string `parquet:"name=description, type=BYTE_ARRAY, convertedtype=UTF8"`
	Body        string `parquet:"name=body, type=BYTE_ARRAY, convertedtype=UTF8"`
	Url         string `parquet:"name=url, type=BYTE_ARRAY, convertedtype=UTF8"`
	DateTime    int64  `parquet:"name=dt, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	Domain      string `parquet:"name=domain, type=BYTE_ARRAY, convertedtype=UTF8"`
}

// need to get right conversion!!!
func (s *NewsArticleRaw) ToArticle() NewsArticle {
	parsedTime, err := time.Parse(time.RFC3339, s.DateTime)
	if err != nil {
		panic("error parsing time: " + err.Error())
	}

	// Convert time.Time to Unix timestamp in milliseconds
	timestamp := parsedTime.UnixMilli()

	return NewsArticle{
		Title:       s.Title,
		Description: s.Description,
		Body:        s.Body,
		Url:         s.Url,
		DateTime:    timestamp,
		Domain:      s.Domain.Name,
	}
}

// Read input json stream and output to csv file (can later be parquet)
func ProcessNewsArticles(scanner *bufio.Scanner, output *os.File) error {
	// Write column names
	writer := csv.NewWriter(output)
	writer.Write([]string{"title", "description", "body", "url", "dt", "domain"})

	// Loop through each line from stdin
	var err error
	var articleRaw NewsArticleRaw
	i := 0
	for scanner.Scan() {
		if i%10000 == 0 {
			log.Println(i)
		}
		i += 1
		line := scanner.Bytes()
		articleRaw = NewsArticleRaw{}
		err = json.Unmarshal(line, &articleRaw)
		if err != nil {
			return fmt.Errorf("error parsing line: " + string(line) + ", " + err.Error())
		}
		article := articleRaw.ToArticle()
		writer.Write([]string{article.Title, article.Description, article.Body, article.Url, article.DateTime, article.Domain})
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
func ProcessNewsArticlesParquet(scanner *bufio.Scanner, output source.ParquetFile, minScore int64) error {
	// Create parquet writer
	// Initialize column builders
	pool := memory.NewGoAllocator()
	stringBuilders := make([]*array.StringBuilder, 5)
	for i := range stringBuilders {
		stringBuilders[i] = array.NewStringBuilder(pool)
	}
	intBuilders := []*array.Int64Builder{array.NewInt64Builder(pool)}

	// Define the schema for Arrow
	fields := []arrow.Field{
		{Name: "title", Type: arrow.BinaryTypes.String},
		{Name: "description", Type: arrow.BinaryTypes.String},
		{Name: "body", Type: arrow.BinaryTypes.String},
		{Name: "url", Type: arrow.BinaryTypes.String},
		{Name: "domain", Type: arrow.BinaryTypes.String},
		// Epoch milliseconds
		{Name: "dt", Type: arrow.PrimitiveTypes.Int64},
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
	var articleRaw NewsArticleRaw
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		articleRaw = NewsArticleRaw{}
		err = json.Unmarshal(line, &articleRaw)
		if err != nil {
			return fmt.Errorf("error parsing line: " + string(line) + ", " + err.Error())
		}
		s := articleRaw.ToArticle()
		// Add to builders
		stringBuilders[0].Append(s.Title)
		stringBuilders[1].Append(s.Description)
		stringBuilders[2].Append(s.Body)
		stringBuilders[3].Append(s.Url)
		stringBuilders[4].Append(s.Domain)
		intBuilders[0].Append(s.DateTime)

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
