// Copyright © Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/chunkedDownloader/core"
)

const (
	CountToEnd = 0

	DefaultDownloadBlockSize = int64(4 * 1024 * 1024) // 4MB

	MaxBlockBlobBlockSize = 4000 * 1024 * 1024

	MaxRetryPerDownloadBody = 5
)

func logThroughput(ctx context.Context, p core.PacerAdmin) {
	interval := 4 * time.Second
	intervalStartTime := time.Now()
	prevBytesTransferred := p.GetTotalTraffic()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
			bytesOnWireMb := float64(float64(p.GetTotalTraffic()-prevBytesTransferred) / (1000 * 1000))
			timeElapsed := time.Since(intervalStartTime).Seconds()
			if timeElapsed != 0 {
				throughput := bytesOnWireMb / float64(timeElapsed)
				fmt.Printf("4-sec throughput: %v MBPS\n", throughput)
			}
			// reset the interval timer and byte count
			intervalStartTime = time.Now()
			prevBytesTransferred = p.GetTotalTraffic()
		}
	}
}

// ===============================================================================================//
func main() {

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	action := os.Args[1]
	blobURL := os.Args[2]
	outputFile := os.Args[3]

	slicePool := core.NewMultiSizeSlicePool(MaxBlockBlobBlockSize)
	cl := core.NewCacheLimiter(4 * 1024 * 1024 * 1024) // 4 GiB
	operationChannel := make(chan func(), 64)
	var pacer core.PacerAdmin = core.NewTokenBucketPacer(8 * 1024 * 1024, int64(0))

	worker := func() {
		for f := range operationChannel {
			f()
		}
	}

	go logThroughput(ctx, pacer)

	for i := 0; i < 64; i++ {
		go worker()
	}

	b, err := blockblob.NewClientWithNoCredential(blobURL, &blockblob.ClientOptions{})
	if err != nil {
		fmt.Printf("Failed: %v\n", err)
		return
	}

	if action == "d" {
		fmt.Println("Downloading")
		_, err = core.DownloadFile(ctx, b, outputFile, 8*1024*1024, slicePool, cl, operationChannel, pacer)
	} else if action == "u" {
		fmt.Println("Uploading")
		err = core.UploadFile(ctx, b, outputFile, 8*1024*1024, slicePool, cl, operationChannel, pacer)
	}

	if err != nil {
		fmt.Printf("Failed: %v\n", err)
		return
	}

	fmt.Println("Success")
}
