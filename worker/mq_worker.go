package worker

import (
	"encoding/json"
	"fmt"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/db"
	"github.com/sa7mon/s3scanner/mq"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
	"sync"
)

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// MQConfig groups the message-queue connection parameters for a WorkMQ
// consumer so they travel together instead of as a long parameter list.
type MQConfig struct {
	ThreadID int
	Conn     *amqp.Connection
	Queue    string
	Threads  int
}

// storeIfEnabled persists b to the database when the run is configured to do so.
func storeIfEnabled(cfg Config, b *bucket.Bucket) {
	if !cfg.WriteToDB {
		return
	}
	if dbErr := db.StoreBucket(b); dbErr != nil {
		log.Error(dbErr)
	}
}

// messageOutcome tells the consumer loop how to proceed after a message has
// been handled.
type messageOutcome int

const (
	// outcomeContinue means move on to the next message on the same channel.
	outcomeContinue messageOutcome = iota
	// outcomeReconnect means abandon this channel and re-establish a new one.
	outcomeReconnect
	// outcomeStop means shut this consumer down (used by the test hook).
	outcomeStop
)

// handleMessage scans a single bucket described by delivery j. It fully
// acknowledges or rejects the delivery and returns how the consumer loop
// should proceed. Splitting this out of WorkMQ keeps the control flow flat and
// each decision named.
func handleMessage(j amqp.Delivery, cfg Config, stopAfterOne bool) messageOutcome {
	bucketToScan := bucket.Bucket{}
	if unmarshalErr := json.Unmarshal(j.Body, &bucketToScan); unmarshalErr != nil {
		log.Error(unmarshalErr)
	}

	if !bucket.IsValidS3BucketName(bucketToScan.Name) {
		log.Info(fmt.Sprintf("invalid   | %s", bucketToScan.Name))
		FailOnError(j.Ack(false), "failed to ack")
		return outcomeContinue
	}

	b, existsErr := cfg.Provider.BucketExists(&bucketToScan)
	if existsErr != nil {
		log.WithFields(log.Fields{"bucket": bucketToScan.Name, "step": "checkExists"}).Error(existsErr)
		FailOnError(j.Reject(false), "failed to reject")
		return outcomeContinue
	}

	if b.Exists == bucket.BucketNotExist {
		// ack the message and skip to the next
		log.Infof("not_exist | %s", b.Name)
		FailOnError(j.Ack(false), "failed to ack")
		return outcomeContinue
	}

	scanErr := cfg.Provider.Scan(b, false)
	if scanErr != nil {
		log.WithFields(log.Fields{"bucket": b}).Error(scanErr)
		FailOnError(j.Reject(false), "failed to reject")
		return outcomeContinue
	}

	if enumerated := enumerateIfNeeded(j, cfg, b, &bucketToScan); !enumerated {
		return outcomeContinue
	}

	PrintResult(&bucketToScan, false)
	if ackErr := j.Ack(false); ackErr != nil {
		// Acknowledge mq message. May fail if we've taken too long and the server has closed the channel.
		// If it has, reconnect and start over with a fresh channel.
		log.WithFields(log.Fields{"bucket": b}).Error(ackErr)
		return outcomeReconnect
	}

	storeIfEnabled(cfg, &bucketToScan)
	if stopAfterOne {
		return outcomeStop
	}
	return outcomeContinue
}

// enumerateIfNeeded runs object enumeration for an existing, readable bucket
// when enumeration is enabled. It reports whether the caller should keep
// processing this delivery (true) or has already acked/rejected it (false).
func enumerateIfNeeded(j amqp.Delivery, cfg Config, b *bucket.Bucket, bucketToScan *bucket.Bucket) bool {
	if !cfg.DoEnumerate {
		return true
	}

	if b.PermAllUsersRead != bucket.PermissionAllowed {
		PrintResult(bucketToScan, false)
		FailOnError(j.Ack(false), "failed to ack")
		storeIfEnabled(cfg, bucketToScan)
		return false
	}

	log.WithFields(log.Fields{"method": "main.mqwork()",
		"bucket_name": b.Name, "region": b.Region}).Debugf("enumerating objects...")

	if enumErr := cfg.Provider.Enumerate(b); enumErr != nil {
		log.Errorf("Error enumerating bucket '%s': %v\nEnumerated objects: %v", b.Name, enumErr, len(b.Objects))
		FailOnError(j.Reject(false), "failed to reject")
		return false
	}
	return true
}

func WorkMQ(wg *sync.WaitGroup, cfg Config, mqCfg MQConfig) {
	_, stopAfterOne := os.LookupEnv("TEST_MQ") // If we're being tested, exit after one bucket is scanned
	defer wg.Done()

	// Wrap the whole thing in a for (while) loop so if the mq server kills the channel, we start it up again
	for {
		ch, chErr := mq.Connect(mqCfg.Conn, mqCfg.Queue, mqCfg.Threads, mqCfg.ThreadID)
		if chErr != nil {
			FailOnError(chErr, "couldn't connect to message queue")
		}

		consumer := fmt.Sprintf("%s_%v", mqCfg.Queue, mqCfg.ThreadID)
		msgs, consumeErr := ch.Consume(mqCfg.Queue, consumer, false, false, false, false, nil)
		if consumeErr != nil {
			log.Error(fmt.Errorf("failed to register a consumer: %w", consumeErr))
			return
		}

		if reconnect := consume(msgs, cfg, stopAfterOne); !reconnect {
			return
		}
	}
}

// consume processes deliveries until the channel closes or a message handler
// signals a reconnect. It returns true when the caller should re-establish the
// channel, or false when the consumer should shut down.
func consume(msgs <-chan amqp.Delivery, cfg Config, stopAfterOne bool) bool {
	for j := range msgs {
		switch handleMessage(j, cfg, stopAfterOne) {
		case outcomeStop:
			return false
		case outcomeReconnect:
			return true
		case outcomeContinue:
		}
	}
	return true
}
