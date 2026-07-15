package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// GetRegionsDO fetches regions from the DigitalOcean docs HTML page.
func GetRegionsDO() ([]string, error) {
	requestURL := "https://docs.digitalocean.com/platform/regional-availability/"
	res, err := http.Get(requestURL)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, fmt.Errorf("status code error: %d %s", res.StatusCode, res.Status)
	}

	doc, err := goquery.NewDocumentFromReader(res.Body)
	if err != nil {
		return nil, err
	}

	regions := regionHeaders(doc)

	var supportedRegions []string
	doc.Find("h2#other-digitalocean-products + div table tbody tr").Each(func(_ int, row *goquery.Selection) {
		// Only the "Spaces" row tells us which regions support object storage.
		if row.Find("td").First().Text() != "Spaces" {
			return
		}
		supportedRegions = append(supportedRegions, spacesRegions(row, regions)...)
	})

	return supportedRegions, nil
}

// regionHeaders returns the region names from the product-availability table header,
// excluding the leading "Product" label column.
func regionHeaders(doc *goquery.Document) []string {
	var regions []string
	doc.Find("h2#other-digitalocean-products + div table thead tr th").Each(func(_ int, t *goquery.Selection) {
		if t.Text() != "Product" {
			regions = append(regions, t.Text())
		}
	})
	return regions
}

// spacesRegions returns the region names (aligned to regions by column index) whose
// cell in the Spaces row is marked as supported.
func spacesRegions(row *goquery.Selection, regions []string) []string {
	var supported []string
	row.Find("td").Each(func(i int, v *goquery.Selection) {
		// A non-empty cell (marked with a circle icon) means Spaces is supported in that region.
		if v.Has("i.fa-circle").Length() != 0 {
			supported = append(supported, strings.ToLower(regions[i-1]))
		}
	})
	return supported
}
