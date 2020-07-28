package main

import (
	"crypto/md5"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"google.golang.org/protobuf/proto"
)

func main() {
	fmt.Printf("Testing aggregation\n")

	client := kinesis.New(session.New(&aws.Config{
		Region: aws.String("us-east-1"),
	}))

	agg := NewAggregator()
	partitionKey := "fakekey2"

	for idx := 0; idx < 5000; idx++ {
		timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.999Z07:00")
		idxStr := strconv.Itoa(idx)
		record := []byte("{\"src_timestamp\": \"" + timestamp + "\", \"component\": \"kinesis-protobuf-test\", \"subcomponent\": \"kinesis-protobuf-test-a-very-long-string-that-keeps-on-going-so-we-fill-the-20k-buffer-faster___this-is-more-data-trying-to-make-the-buffer-bigger-blah-blah-blah-blah-blah-blah-blah-blah!!!!!!!!!!!!!!!!!!\", \"seqNum\": " + idxStr + "}")
		//fmt.Printf("record len (%d) \n", len(record))
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

var (
	kclMagicNumber    = []byte{0xF3, 0x89, 0x9A, 0xC2}
	kclMagicNumberLen = len(kclMagicNumber)
)

const (
	maximumRecordSize       = 1024 * 1024 // 1 MB
	defaultMaxAggRecordSize = 20 * 1024   // 20K
	pKeyIdxSize             = 8
	aggProtobufBytes        = 2 // Marshalling the data into protobuf adds 2 bytes
)

// Aggregator kinesis aggregator
type Aggregator struct {
	partitionKeys    map[string]uint64
	partitionKey     string
	records          []*Record
	aggSize          int // Size of both records, and partitionKeys in bytes
	maxAggRecordSize int
}

// NewAggregator create a new aggregator
func NewAggregator() *Aggregator {

	return &Aggregator{
		partitionKeys:    make(map[string]uint64, 0),
		records:          make([]*Record, 0),
		maxAggRecordSize: defaultMaxAggRecordSize,
	}
}

// AddRecord to the aggregate buffer.
// Will return a kinesis PutRecordsRequest once buffer is full, or if the data exceeds the aggregate limit.
func (a *Aggregator) AddRecord(partitionKey string, data []byte) (entry *kinesis.PutRecordsRequestEntry, err error) {

	partitionKeySize := len([]byte(partitionKey))
	dataSize := len(data)

	// If this is a very large record, then don't aggregate it.
	if dataSize >= a.maxAggRecordSize {
		return &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: aws.String(partitionKey),
		}, nil
	}

	if a.getSize()+dataSize+partitionKeySize+pKeyIdxSize >= maximumRecordSize {
		// Aggregate records, and return
		entry, err = a.AggregateRecords()
		if err != nil {
			return entry, err
		}
		// Clear buffer
		a.clearBuffer()
	}

	// Add new record, and update aggSize
	partitionKeyIndex := a.addPartitionKey(partitionKey)

	a.records = append(a.records, &Record{
		Data:              data,
		PartitionKeyIndex: &partitionKeyIndex,
	})

	a.aggSize += dataSize + pKeyIdxSize

	return entry, err
}

// AggregateRecords will flush proto-buffered records into a put request
func (a *Aggregator) AggregateRecords() (entry *kinesis.PutRecordsRequestEntry, err error) {

	pkeys := a.getPartitionKeys()

	fmt.Printf("Found (%d) pkeys: %v\n", len(pkeys), pkeys)
	agg := &AggregatedRecord{
		PartitionKeyTable: a.getPartitionKeys(),
		Records:           a.records,
	}

	protoBufData, err := proto.Marshal(agg)
	if err != nil {
		fmt.Println("Failed to encode address book:", err)
		return nil, err
	}

	fmt.Printf("protoBufData (%d)\n", len(protoBufData))

	md5Sum := md5.New()
	md5Sum.Write(protoBufData)
	md5CheckSum := md5Sum.Sum(nil)

	kclData := append(kclMagicNumber, protoBufData...)
	kclData = append(kclData, md5CheckSum...)

	fmt.Printf("Aggregating (%d) records of size (%d) with total size (%d)\n", len(a.records), a.getSize(), len(kclData))

	return &kinesis.PutRecordsRequestEntry{
		Data:         kclData,
		PartitionKey: aws.String(a.partitionKey),
	}, nil
}

// GetRecordCount gets number of buffered records
func (a *Aggregator) GetRecordCount() int {
	return len(a.records)
}

func (a *Aggregator) addPartitionKey(partitionKey string) uint64 {
	if a.partitionKey == "" {
		a.partitionKey = partitionKey
	}

	if idx, ok := a.partitionKeys[partitionKey]; ok {
		return idx
	}

	idx := uint64(len(a.partitionKeys))
	a.partitionKeys[partitionKey] = idx
	a.aggSize += len([]byte(partitionKey))
	return idx
}

func (a *Aggregator) getPartitionKeys() []string {
	keys := make([]string, 0)
	for pk := range a.partitionKeys {
		keys = append(keys, pk)
	}
	return keys
}

// getSize of protobuf records, partitionKeys, magicNumber, and md5sum in bytes
func (a *Aggregator) getSize() int {
	return a.aggSize + kclMagicNumberLen + md5.Size + aggProtobufBytes
}

func (a *Aggregator) clearBuffer() {
	a.partitionKey = ""
	a.partitionKeys = make(map[string]uint64, 0)
	a.records = make([]*Record, 0)
	a.aggSize = 0
}
