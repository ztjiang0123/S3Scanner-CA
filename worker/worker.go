package worker

import (
	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/db"
	"github.com/sa7mon/s3scanner/provider"
	log "github.com/sirupsen/logrus"
	"sync"
)

func PrintResult(b *bucket.Bucket, json bool) {
	if json {
		log.WithField("bucket", b).Info()
		return
	}

	if b.Exists == bucket.BucketNotExist {
		log.Infof("not_exist | %s", b.Name)
		return
	}

	result := fmt.Sprintf("exists    | %v | %v | %v", b.Name, b.Region, b.String())
	if b.ObjectsEnumerated {
		result = fmt.Sprintf("%v | %v objects (%v)", result, len(b.Objects), humanize.Bytes(b.BucketSize))
	}
	log.Info(result)
}

// Config groups the behavior flags that control a Work run. These flags always
// travel together from the caller, so bundling them keeps Work's signature short
// and its call sites easy to read.
type Config struct {
	DoEnumerate bool
	WriteToDB   bool
	JSON        bool
}

func Work(wg *sync.WaitGroup, buckets chan bucket.Bucket, provider provider.StorageProvider, cfg Config) {
	defer wg.Done()
	for b1 := range buckets {
		b, existsErr := provider.BucketExists(&b1)
		if existsErr != nil {
			log.Errorf("error     | %s | %s", b.Name, existsErr.Error())
			continue
		}

		if b.Exists == bucket.BucketNotExist {
			PrintResult(b, cfg.JSON)
			continue
		}

		// Scan permissions
		scanErr := provider.Scan(b, false)
		if scanErr != nil {
			log.WithFields(log.Fields{"bucket": b}).Error(scanErr)
		}

		if cfg.DoEnumerate && b.PermAllUsersRead == bucket.PermissionAllowed {
			log.WithFields(log.Fields{"method": "main.work()",
				"bucket_name": b.Name, "region": b.Region}).Debugf("enumerating objects...")
			enumErr := provider.Enumerate(b)
			if enumErr != nil {
				log.Errorf("Error enumerating bucket '%s': %v\nEnumerated objects: %v", b.Name, enumErr, len(b.Objects))
				continue
			}
		}
		PrintResult(b, cfg.JSON)

		if cfg.WriteToDB {
			dbErr := db.StoreBucket(b)
			if dbErr != nil {
				log.Error(dbErr)
			}
		}
	}
}
