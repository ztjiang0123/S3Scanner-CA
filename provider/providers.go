package provider

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/permission"
	"github.com/sa7mon/s3scanner/provider/clientmap"
	log "github.com/sirupsen/logrus"
)

const (
	PathStyle        = 0
	VirtualHostStyle = 1
)

type StorageProvider interface {
	Insecure() bool
	AddressStyle() int
	BucketExists(*bucket.Bucket) (*bucket.Bucket, error)
	Scan(*bucket.Bucket, bool) error
	Enumerate(*bucket.Bucket) error
	Name() string
}

type bucketCheckResult struct {
	region string
	exists bool
}

var AllProviders = []string{
	"aws", "custom", "digitalocean", "dreamhost", "gcp", "linode", "scaleway",
}

var ProviderRegions = map[string][]string{
	"digitalocean": {"ams3", "atl1", "blr1", "fra1", "lon1", "nyc3", "sfo2", "sfo3", "sgp1", "syd1", "tor1"},
	"dreamhost":    {"us-east-1"},
	"linode": {"us-east-1", "us-ord-1", "us-lax-1", "us-sea-1", "us-southeast-1", "us-mia-1", "us-sea-9",
		"us-iad-1", "us-iad-10", "id-cgk-1", "in-maa-1", "in-bom-1", "jp-osa-1", "jp-tyo-1", "ap-south-1", "sg-sin-1",
		"eu-central-1", "de-fra-1", "es-mad-1", "fr-par-1", "gb-lon-1", "it-mil-1", "nl-ams-1", "se-sto-1",
		"au-mel-1", "br-gru-1", "us-lax-4", "us-ord-10"},
	"scaleway": {"fr-par", "nl-ams", "pl-waw"},
	"wasabi": {"us-west-1", "us-east-1", "us-east-2", "us-central-1", "ca-central-1", "eu-west-1", "eu-west-2",
		"eu-west-3", "eu-central-1", "eu-central-2", "eu-south-1", "ap-northeast-1", "ap-northeast-2", "ap-southeast-2",
		"ap-southeast-1"},
}

func NewProvider(name string) (StorageProvider, error) {
	var (
		provider StorageProvider
		err      error
	)
	switch name {
	case "aws":
		provider, err = NewProviderAWS()
	case "digitalocean":
		provider, err = NewDigitalOcean()
	case "dreamhost":
		provider, err = NewProviderDreamhost()
	case "gcp":
		provider, err = NewProviderGCP()
	case "linode":
		provider, err = NewProviderLinode()
	case "scaleway":
		provider, err = NewProviderScaleway()
	case "wasabi":
		provider, err = NewProviderWasabi()
	default:
		err = fmt.Errorf("unknown provider: %s", name)
	}
	return provider, err
}

func newNonAWSClient(sp StorageProvider, regionURL string) (*s3.Client, error) {
	var httpClient s3.HTTPClient

	if sp.Insecure() {
		httpClient = &http.Client{Transport: &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}}
	} else {
		httpClient = &http.Client{Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
		}}
	}

	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
		config.WithHTTPClient(httpClient),
		config.WithRegion("auto"),
	)
	if err != nil {
		return nil, err
	}

	addrStyleOption := func(o *s3.Options) { o.UsePathStyle = false }
	if sp.AddressStyle() == PathStyle {
		addrStyleOption = func(o *s3.Options) { o.UsePathStyle = true }
	}

	cfg.BaseEndpoint = aws.String(regionURL)
	cfg.Credentials = nil // TODO: Remove and test
	return s3.NewFromConfig(cfg, addrStyleOption), nil
}

// setBucketExistence records the result of a bucketExists* check on b. It is
// shared by every provider's BucketExists implementation so the exists/region
// bookkeeping lives in exactly one place.
func setBucketExistence(b *bucket.Bucket, exists bool, region string) {
	if exists {
		b.Exists = bucket.BucketExists
		b.Region = region
		return
	}
	b.Exists = bucket.BucketNotExist
}

// buildRegionClients creates one anonymous S3 client per region in regions,
// using endpointFor to build each region's endpoint URL. It replaces the
// near-identical newClients() bodies previously copied into every non-AWS
// provider.
func buildRegionClients(sp StorageProvider, regions []string, endpointFor func(region string) string) (*clientmap.ClientMap, error) {
	clients := clientmap.WithCapacity(len(regions))
	for _, r := range regions {
		client, err := newNonAWSClient(sp, endpointFor(r))
		if err != nil {
			return nil, err
		}
		clients.Set(r, false, client)
	}
	return clients, nil
}

// initClients builds a provider p that stores its ClientMap in the field
// pointed to by clientsField, populating it via newClients. It captures the
// identical "new, populate clients, return" constructor body shared by the
// non-AWS providers so each NewProvider* is a one-line call.
func initClients[T StorageProvider](p T, clientsField **clientmap.ClientMap, newClients func() (*clientmap.ClientMap, error)) (T, error) {
	clients, err := newClients()
	if err != nil {
		return p, err
	}
	*clientsField = clients
	return p, nil
}

// enumerateBucketObjects performs the guard-and-enumerate sequence shared by the
// non-AWS providers' Enumerate implementations: it verifies the bucket exists,
// resolves the region client, and lists its objects.
func enumerateBucketObjects(clients *clientmap.ClientMap, b *bucket.Bucket) error {
	if b.Exists != bucket.BucketExists {
		return errors.New("bucket might not exist")
	}
	client := clients.Get(b.Region, false)
	return enumerateListObjectsV2(client, b)
}

/*
enumerateListObjectsV2 will enumerate all objects stored in b using the ListObjectsV2 API endpoint. The endpoint will
be called until the IsTruncated field is false
*/
func enumerateListObjectsV2(client *s3.Client, b *bucket.Bucket) error {
	var continuationToken *string
	page := 0

	for {
		log.WithFields(log.Fields{
			"bucket_name": b.Name,
			"method":      "providers.enumerateListObjectsV2()",
		}).Debugf("requesting objects page %d", page)
		output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:            &b.Name,
			ContinuationToken: continuationToken,
			EncodingType:      types.EncodingTypeUrl,
		},
		)
		if err != nil {
			return err
		}

		for _, obj := range output.Contents {
			var key string
			if obj.Key != nil {
				key = *obj.Key
			}
			var size uint64
			if obj.Size != nil {
				size = uint64(*obj.Size)
			}
			b.Objects = append(b.Objects, bucket.Object{Key: key, Size: size})
			b.BucketSize += size
		}

		if output.IsTruncated == nil || !*output.IsTruncated {
			b.ObjectsEnumerated = true
			break
		}
		continuationToken = output.NextContinuationToken
		page++
		if page >= 5000 { // TODO: Should this limit be lowered?
			return errors.New("more than 5000 pages of objects found. Skipping for now")
		}
	}
	b.NumObjects = int32(len(b.Objects))
	return nil
}

func checkPermissions(client *s3.Client, b *bucket.Bucket, doDestructiveChecks bool) error {
	/*
		// 1. Check if b exists
		// 2. Check for READ_ACP
		// 3. If FullControl is allowed for either AllUsers or AuthorizedUsers, skip the remainder of those tests
		// 4. Check for READ
		// 5. If doing destructive checks:
		// 5a. Check for Write
		// 5b. Check for WriteACP
	*/

	b.DateScanned = time.Now()

	// Check for READ_ACP permission
	canReadACL, err := permission.CheckPermReadACL(client, b)
	if err != nil {
		return fmt.Errorf("error occurred while checking for ReadACL: %v", err.Error())
	}
	b.PermAllUsersReadACL = bucket.Permission(canReadACL)
	// TODO: Can we skip the rest of the checks if READ_ACP is allowed?

	// We can skip the rest of the checks if FullControl is allowed
	if b.PermAuthUsersFullControl == bucket.PermissionAllowed {
		return nil
	}

	// Check for READ permission
	canRead, err := permission.CheckPermRead(client, b)
	if err != nil {
		return fmt.Errorf("%v | error occurred while checking for READ: %v", b.Name, err.Error())
	}
	b.PermAllUsersRead = bucket.Permission(canRead)

	if doDestructiveChecks {
		// Check for WRITE permission
		permWrite, writeErr := permission.CheckPermWrite(client, b)
		if writeErr != nil {
			return fmt.Errorf("%v | error occurred while checking for WRITE: %v", b.Name, writeErr.Error())
		}
		b.PermAllUsersWrite = bucket.Permission(permWrite)

		// Check for WRITE_ACP permission
		permWriteACL, writeACLErr := permission.CheckPermWriteACL(client, b)
		if writeACLErr != nil {
			return fmt.Errorf("error occurred while checking for WriteACL: %v", writeACLErr.Error())
		}
		b.PermAllUsersWriteACL = bucket.Permission(permWriteACL)
	}
	return nil
}

// probeRegion issues the region-existence request for b against a single region
// client. Scaleway needs a different request than the other providers.
func probeRegion(client *s3.Client, b *bucket.Bucket) error {
	// Unlike other APIs, Scaleway returns '200 OK' to a HEAD request sent to the wrong region for a
	// bucket that does exist in another region. So instead, we send a GET request for a list of 1 object.
	// Scaleway will return 404 to the GET request in any region other than the one the bucket belongs to.
	// See https://github.com/sa7mon/S3Scanner/issues/209 for a better way to fix this.
	if b.Provider == "scaleway" {
		_, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
			Bucket:  &b.Name,
			MaxKeys: aws.Int32(1),
		})
		return err
	}
	_, err := manager.GetBucketRegion(context.TODO(), client, b.Name)
	return err
}

// classifyRegionError interprets the error from a region probe. It returns
// (result, handled): when handled is true, result is the existence answer for
// that region; when handled is false, err is a genuine failure to surface.
func classifyRegionError(regionErr error, region string, logFields log.Fields) (bucketCheckResult, bool, error) {
	var bnf manager.BucketNotFound // Can be returned from GetBucketRegion()
	var nsb *types.NoSuchBucket    // Can be returned from ListObjectsV2()
	var re2 *awshttp.ResponseError

	switch {
	case errors.As(regionErr, &bnf), errors.As(regionErr, &nsb):
		log.WithFields(logFields).Debugf("BucketNotFound")
		return bucketCheckResult{region: region, exists: false}, true, nil
	case errors.As(regionErr, &re2) && re2.HTTPStatusCode() == 403:
		log.WithFields(logFields).Debugf("AccessDenied")
		return bucketCheckResult{region: region, exists: true}, true, nil
	default:
		// If regionErr is a ResponseError, only return the unwrapped error i.e. "Method Not Allowed"
		// Otherwise, return the whole error
		err := regionErr
		if errors.As(regionErr, &re2) {
			err = re2.Unwrap()
		}
		log.WithFields(logFields).Debug(fmt.Errorf("unhandled error: %w", regionErr))
		return bucketCheckResult{}, false, err
	}
}

// checkRegion probes a single region for b and reports the outcome on results
// (existence known) or e (unexpected failure).
func checkRegion(client *s3.Client, region string, b *bucket.Bucket, results chan<- bucketCheckResult, e chan<- error) {
	logFields := log.Fields{
		"bucket_name": b.Name,
		"region":      region,
		"method":      "providers.bucketExists()",
	}

	regionErr := probeRegion(client, b)
	if regionErr == nil {
		log.WithFields(logFields).Debugf("no error - bucket exists")
		results <- bucketCheckResult{region: region, exists: true}
		return
	}

	res, handled, err := classifyRegionError(regionErr, region, logFields)
	if handled {
		results <- res
		return
	}
	e <- err
}

// bucketExists takes a bucket name and checks if it exists in any region contained in clients
func bucketExists(clients *clientmap.ClientMap, b *bucket.Bucket) (bool, string, error) {
	results := make(chan bucketCheckResult, clients.Len())
	e := make(chan error, 1)

	clients.Each(func(region string, _ bool, client *s3.Client) {
		go checkRegion(client, region, b, results, e)
	})

	for i := 0; i < clients.Len(); i++ {
		select {
		case err := <-e:
			return false, "", err
		case res := <-results:
			if res.exists {
				return true, res.region, nil
			}
		}
	}
	return false, "", nil
}

// bucketExists301 takes a bucket name and checks if it exists. It assumes the server will respond with a 301 status
// and `x-amz-bucket-region` header pointing to the correct region if an incorrect region is specified.
func bucketExists301(client *s3.Client, region string, b *bucket.Bucket) (bool, string, error) {
	logFields := log.Fields{
		"bucket_name": b.Name,
		"region":      region,
		"method":      "providers.bucketExists301()",
	}

	bucketURL, err := url.JoinPath(*client.Options().BaseEndpoint, b.Name)
	if err != nil {
		return false, "", logErr(logFields, err)
	}
	req, reqErr := http.NewRequest("HEAD", bucketURL, nil)
	if reqErr != nil {
		return false, "", logErr(logFields, reqErr)
	}
	res, resErr := client.Options().HTTPClient.Do(req)
	if resErr != nil {
		return false, "", logErr(logFields, resErr)
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case 200:
		return true, region, nil
	case 301:
		return true, res.Header.Get("x-amz-bucket-region"), nil
	case 403:
		return true, region, nil
	case 404:
		return false, "", nil
	}
	return false, "", logErr(logFields, fmt.Errorf("unexpected status code: %d", res.StatusCode))
}

func logErr(fields log.Fields, err error) error {
	log.WithFields(fields).Error(err.Error())
	return err
}
