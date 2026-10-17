// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package archive

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blairham/k4a/internal/awscreds"
)

// S3API is the subset of *s3.Client the estimate calls.
type S3API interface {
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// estimateConcurrency bounds parallel LIST calls; a month of hourly
// partitions is 720 prefixes.
const estimateConcurrency = 8

// Estimate sums the sizes of every object under prefixes (s3:// URIs). For
// row formats that is what Athena will scan; for columnar formats it is an
// overestimate, the safe direction for a confirm threshold.
func Estimate(ctx context.Context, api S3API, prefixes []string) (int64, error) {
	var (
		mu       sync.Mutex
		total    int64
		firstErr error
		wg       sync.WaitGroup
	)
	type target struct{ uri, bucket, prefix string }
	targets := make([]target, 0, len(prefixes))
	for _, uri := range prefixes {
		bucket, prefix, err := splitS3URI(uri)
		if err != nil {
			return 0, err
		}
		targets = append(targets, target{uri, bucket, prefix})
	}
	sem := make(chan struct{}, estimateConcurrency)
	for _, tg := range targets {
		uri, bucket, prefix := tg.uri, tg.bucket, tg.prefix
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			n, err := sumPrefix(ctx, api, bucket, prefix)
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("listing %s: %w", uri, err)
			}
			total += n
		}()
	}
	wg.Wait()
	return total, firstErr
}

func sumPrefix(ctx context.Context, api S3API, bucket, prefix string) (int64, error) {
	pager := s3.NewListObjectsV2Paginator(api, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	var n int64
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return n, err
		}
		for _, o := range page.Contents {
			n += aws.ToInt64(o.Size)
		}
	}
	return n, nil
}

func splitS3URI(uri string) (bucket, prefix string, err error) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return "", "", fmt.Errorf("%q is not an s3:// URI", uri)
	}
	bucket, prefix, _ = strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("%q has no bucket", uri)
	}
	return bucket, prefix, nil
}

// NewClients builds the Athena and S3 clients for profile and region
// through awscreds, the same in-process shared-config loading IAM auth and
// the SSM lookup use.
func NewClients(ctx context.Context, profile, region string) (AthenaAPI, S3API, error) {
	cfg, err := awscreds.Load(ctx, profile, region)
	if err != nil {
		return nil, nil, err
	}
	return athena.NewFromConfig(cfg), s3.NewFromConfig(cfg), nil
}
