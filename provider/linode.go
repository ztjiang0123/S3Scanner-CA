package provider

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/provider/clientmap"
)

type Linode struct {
	clients *clientmap.ClientMap
}

func NewProviderLinode() (*Linode, error) {
	pl := new(Linode)
	return initClients(pl, &pl.clients, pl.newClients)
}

func (pl *Linode) getRegionClient(region string) *s3.Client {
	return pl.clients.Get(region, false)
}

func (pl *Linode) BucketExists(b *bucket.Bucket) (*bucket.Bucket, error) {
	b.Provider = pl.Name()
	exists, region, err := bucketExists(pl.clients, b)
	if err != nil {
		return b, err
	}
	setBucketExistence(b, exists, region)
	return b, nil
}

func (pl *Linode) Enumerate(b *bucket.Bucket) error {
	return enumerateBucketObjects(pl.clients, b)
}

func (pl *Linode) newClients() (*clientmap.ClientMap, error) {
	return buildRegionClients(pl, ProviderRegions[pl.Name()], func(r string) string {
		return fmt.Sprintf("https://%s.linodeobjects.com", r)
	})
}

func (pl *Linode) Scan(b *bucket.Bucket, doDestructiveChecks bool) error {
	client := pl.getRegionClient(b.Region)
	return checkPermissions(client, b, doDestructiveChecks)
}

func (*Linode) Insecure() bool {
	return false
}

func (*Linode) Name() string {
	return "linode"
}

func (*Linode) AddressStyle() int {
	return VirtualHostStyle
}
