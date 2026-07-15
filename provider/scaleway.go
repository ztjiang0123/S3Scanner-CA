package provider

import (
	"fmt"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/provider/clientmap"
)

type Scaleway struct {
	clients *clientmap.ClientMap
}

func NewProviderScaleway() (*Scaleway, error) {
	sc := new(Scaleway)
	return initClients(sc, &sc.clients, sc.newClients)
}

func (sc *Scaleway) newClients() (*clientmap.ClientMap, error) {
	return buildRegionClients(sc, ProviderRegions[sc.Name()], func(r string) string {
		return fmt.Sprintf("https://s3.%s.scw.cloud", r)
	})
}

func (sc *Scaleway) Scan(b *bucket.Bucket, doDestructiveChecks bool) error {
	client := sc.clients.Get(b.Region, false)
	return checkPermissions(client, b, doDestructiveChecks)
}

func (*Scaleway) Insecure() bool {
	return false
}

func (*Scaleway) Name() string {
	return "scaleway"
}

func (*Scaleway) AddressStyle() int {
	return PathStyle
}

func (sc *Scaleway) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	b.Provider = sc.Name()
	exists, region, err := bucketExists(sc.clients, b)
	if err != nil {
		return b, err
	}
	setBucketExistence(b, exists, region)
	return b, nil
}

func (sc *Scaleway) Enumerate(b *bucket.Bucket) error {
	return enumerateBucketObjects(sc.clients, b)
}
