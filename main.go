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
	"io"
	"os"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

const (
	CountToEnd = 0

	DefaultDownloadBlockSize = int64(4 * 1024 * 1024) // 4MB

	MaxBlockBlobBlockSize = 4000 * 1024 * 1024

	MaxRetryPerDownloadBody = 5
)

func getBlobPropertiesOptionsFromDownloadOptions(o *blob.DownloadFileOptions) *blob.GetPropertiesOptions{
	if o == nil {
		return nil
	}
	return &blob.GetPropertiesOptions{
		AccessConditions: o.AccessConditions,
		CPKInfo:          o.CPKInfo,
	}
}

func getDownloadBlobOptions(o *blob.DownloadFileOptions, rnge blob.HTTPRange, rangeGetContentMD5 *bool) *blob.DownloadStreamOptions {
	if o == nil {
		return nil
	}
	return &blob.DownloadStreamOptions{
		AccessConditions:   o.AccessConditions,
		CPKInfo:            o.CPKInfo,
		CPKScopeInfo:       o.CPKScopeInfo,
		Range:              rnge,
		RangeGetContentMD5: rangeGetContentMD5,
	}
}

func DownloadFile(ctx context.Context,
		  file *os.File,
		  o *blob.DownloadFileOptions,
		  b *blob.Client,
		  s ByteSlicePooler,
		  c CacheLimiter) (int64, error) {
	if o == nil {
		o = &blob.DownloadFileOptions{}
	}

	// 1. Calculate the size of the destination file
	var size int64

	count := o.Range.Count
	if count == CountToEnd {
		// Try to get Azure blob's size
		getBlobPropertiesOptions := getBlobPropertiesOptionsFromDownloadOptions(o)
		props, err := b.GetProperties(ctx, getBlobPropertiesOptions)
		if err != nil {
			return 0, err
		}
		size = *props.ContentLength - o.Range.Offset
	} else {
		size = count
	}

	// 2. Compare and try to resize local file's size if it doesn't match Azure blob's size.
	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if stat.Size() != size {
		if err = file.Truncate(size); err != nil {
			return 0, err
		}
	}

	if size == 0 {
		return 0, nil
	}

	return Download(ctx, b, file, o, s, c, nil)
}

func Download(ctx context.Context,
	      b *blob.Client,
	      file io.WriteCloser,
	      o *blob.DownloadFileOptions,
	      slicePool ByteSlicePooler,
	      cacheLimiter CacheLimiter,
	      operationChannel chan<- func ()) (int64, error) {

	var err error
	errorChannel := make(chan error)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if o.BlockSize == 0 {
		o.BlockSize = DefaultDownloadBlockSize
	}


	count := o.Range.Count
	if count == CountToEnd { // If size not specified, calculate it
		// If we don't have the length at all, get it
		gr, err := b.GetProperties(ctx, getBlobPropertiesOptionsFromDownloadOptions(o))
		if err != nil {
			return 0, err
		}
		count = *gr.ContentLength - o.Range.Offset
	}
	
	if count <= 0 {
		// The file is empty, there is nothing to download.
		return 0, nil
	}
	
	numChunks := uint16(((count - 1) / o.BlockSize) + 1)
	postError := func(err error) {
		select {
		case errorChannel <- err:
		default:
		}
	}

	cf := NewChunkedFileWriter(ctx, slicePool, cacheLimiter, file, uint32(numChunks), MaxRetryPerDownloadBody, false)
	var wg sync.WaitGroup

	// Prepare and do parallel download.
	progress := int64(0)
	progressLock := &sync.Mutex{}

	go func() {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case err = <- errorChannel:
			cancel()
			return
		}
	}()

	for chunkNum := uint16(0); chunkNum < numChunks; chunkNum++ {
		wg.Add(1)

		curChunkSize := o.BlockSize
		if chunkNum == numChunks-1 { // Last chunk
			curChunkSize = count - (int64(chunkNum) * o.BlockSize) // Remove size of all transferred chunks from total
		}
		
		offset := int64(chunkNum) * o.BlockSize

		operationChannel <- func() {
			defer wg.Done()

			select {
			case <-ctx.Done():
				return
			default:
			}

			downloadBlobOptions := getDownloadBlobOptions(o, blob.HTTPRange{
				Offset: offset + o.Range.Offset,
				Count:  curChunkSize,
			}, nil)

			err = cf.WaitToScheduleChunk(ctx, curChunkSize)
			if err != nil {
				postError(err)
				return
			}

			dr, err := b.DownloadStream(ctx, downloadBlobOptions)
			if err != nil {
				postError(err)
				return
			}
			var body io.ReadCloser = dr.NewRetryReader(ctx, &o.RetryReaderOptionsPerBlock)
			if o.Progress != nil {
				rangeProgress := int64(0)
				body = streaming.NewResponseProgress(
					body,
					func(bytesTransferred int64) {
						diff := bytesTransferred - rangeProgress
						rangeProgress = bytesTransferred
						progressLock.Lock()
						progress += diff
						o.Progress(progress)
						progressLock.Unlock()
					})
			}

			cf.EnqueueChunk(ctx, offset, curChunkSize, body, true)
			if err != nil {
				postError(err)
				return
			}
			
			body.Close()
		}
	}

	if err != nil {
		return 0, err
	}

	_, err = cf.Flush(ctx)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func main() {

	ctx := context.Background()
	outputFile := os.Args[2]
	blobURL := os.Args[1]

	s := NewMultiSizeSlicePool(MaxBlockBlobBlockSize)
	c   := NewCacheLimiter(4 * 1024 * 1024 * 1024) // 4 GiB

	b, err := blob.NewClientWithNoCredential(blobURL, nil)
	if err != nil {
		fmt.Printf("Failed: %v\n", err)
		return
	}

	fo, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Failed: %v\n", err)
		return
	}
	defer fo.Close()

	_, err = DownloadFile(ctx, fo, nil, b, s, c)
	if err != nil {
		fmt.Printf("Failed: %v\n", err)
		return
	}

	fmt.Println("Success")
	return
}