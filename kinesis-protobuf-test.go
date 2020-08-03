package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/zackwine/kinesis-protobuf-test/aggregate"
)

func main() {
	fmt.Printf("Testing aggregation\n")

	client := kinesis.New(session.New(&aws.Config{
		Region: aws.String("us-east-1"),
	}))

	agg := aggregate.NewAggregator()
	partitionKey1 := "fakekey1"
	partitionKey2 := "fakekey2"
	partitionKey3 := "fakekey3"
	var partitionKey string

	for idx := 0; idx < 5000; idx++ {
		timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.999Z07:00")
		idxStr := strconv.Itoa(idx)
		record := []byte("{\"src_timestamp\": \"" + timestamp + "\", \"component\": \"kinesis-protobuf-test\", \"subcomponent\": \"kinesis-protobuf-test-a-very-long-string-that-keeps-on-going-so-we-fill-the-20k-buffer-faster___this-is-more-data-trying-to-make-the-buffer-bigger-blah-blah-blah-blah-blah-blah-blah-blah!!!!!!!!!!!!!!!!!!\", \"seqNum\": " + idxStr + "}")
		//fmt.Printf("record len (%d) \n", len(record))
		if idx%3 == 0 {
			partitionKey = partitionKey1
		} else if idx%3 == 1 {
			partitionKey = partitionKey2
		} else {
			partitionKey = partitionKey3
		}

		aggRecord, err := agg.AddRecord(partitionKey, record)

		if err != nil {
			fmt.Printf("Failed to add record %v \n", err)
			return
		}

		if aggRecord != nil {
			fmt.Printf("Sending @ idx=%d \n", idx)
			out, err := client.PutRecords(&kinesis.PutRecordsInput{
				StreamName: aws.String("everest-cdvr"),
				Records:    []*kinesis.PutRecordsRequestEntry{aggRecord},
			})

			if err != nil {
				fmt.Println("Failed to put records:", err)
			} else {
				fmt.Println("Output: ", out)
			}
		}
	}

	// 1 * 320 = 337 == 17 extra
	// 2 * 320 = 665 == 25 extra
	// 3 * 320 = 993 == 33 extra
	// 4 * 320 = 1321 == 41 extra

	aggRecord, err := agg.AggregateRecords()
	if err != nil {
		fmt.Printf("Failed to aggregate records %v \n", err)
		return
	}

	out, err := client.PutRecords(&kinesis.PutRecordsInput{
		StreamName: aws.String("everest-cdvr"),
		Records:    []*kinesis.PutRecordsRequestEntry{aggRecord},
	})

	if err != nil {
		fmt.Println("Failed to put records:", err)
	} else {
		fmt.Println("Output: ", out)
	}

}
