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
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
)

const (
	CountToEnd = 0

	DefaultDownloadBlockSize = int64(4 * 1024 * 1024) // 4MB

	MaxBlockBlobBlockSize = 4000 * 1024 * 1024

	MaxRetryPerDownloadBody = 5
)

func DownloadFile(ctx context.Context,
		  		  file *os.File,
				  blockSize int64,
				  b *blob.Client,
				  s ByteSlicePooler,
				  c CacheLimiter,
				  o chan<- func()) (int64, error) {
	// 1. Calculate the size of the destination file
	var size int64
	props, err := b.GetProperties(ctx, nil)
	if err != nil {
		return 0, err
	}
	size = *props.ContentLength

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

	// Nothing to be done
	if size == 0 {
		return 0, nil
	}

	if size <= blockSize { //perform a single thread copy here.
		dr, err := b.DownloadStream(ctx, nil)
		if err != nil {
			return 0, err
		}
		var body io.ReadCloser = dr.NewRetryReader(ctx, nil)
		defer body.Close()

		return io.Copy(file, body)
	}

	return downloadInternal(ctx, b, file, size, blockSize, s, c, o)
}

func downloadInternal(ctx context.Context,
	      			  b *blob.Client,
	     			  file io.WriteCloser,
	      			  fileSize int64,
	     			  blockSize int64,
	     			  slicePool ByteSlicePooler,
	     			  cacheLimiter CacheLimiter,
	     			  operationChannel chan<- func ()) (int64, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var err error
	errorChannel := make(chan error)
	go func() {
		// This goroutine will monitor above channel and
		// cancel the context if any block reports error
		select {
		case <-ctx.Done():
			return
		case err = <- errorChannel:
			cancel()
			return
		}
	}()

	// short hand for routines to report and error
	postError := func(err error) {
		select {
		case <-ctx.Done():
		case errorChannel <- err:
		}
	}
	
	// to synchronize all block scheduling and writer threads.
	var wg sync.WaitGroup

	// file serial writer
	count := fileSize
	numBlocks := uint16(((count - 1) / blockSize) + 1)
	
	blocks := make([]chan []byte, numBlocks)
	totalWrite := int64(0)
	go func() {
		for _, block := range blocks {
			select {
			case <-ctx.Done():
				return
			case buff := <- block:
				n, err := file.Write(buff) 
				if err != nil {
					postError(err)
					return
				}
				if n != len(buff) {
					postError(io.ErrShortWrite)
					return
				}

				slicePool.ReturnSlice(buff)
				totalWrite += int64(n)
			}
		}

		if totalWrite != count {
			postError(io.ErrShortWrite)
		}
	}()

	 // DownloadBlock func downloads each block of the blob into buffer provided
	downloadBlock := func(buff []byte, blockNum uint16, currentBlockSize, offset int64) {
		defer wg.Done()

		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := b.DownloadBuffer(ctx, buff, &blob.DownloadBufferOptions{
			Concurrency: 1,
			BlockSize: blockSize,
			Range: blob.HTTPRange{Offset: offset, Count: currentBlockSize},
		});

		if err != nil {
			postError(err)
			return
		}

		if int64(n) != currentBlockSize {
			postError(errors.New("invalid read"))
			return
		}

		// Send to the filewriter
		blocks[blockNum] <- buff
	}

	for blockNum := uint16(0); blockNum < numBlocks; blockNum++ {
		wg.Add(1)

		currBlockSize := blockSize
		if blockNum == numBlocks-1 { // Last block
			 // Remove size of all transferred blocks from total
			currBlockSize = count - (int64(blockNum) * blockSize)
		}
		
		offset := int64(blockNum) * blockSize

		// allocate a buffer. This buffer will be released by the fileWriter
		if err := cacheLimiter.WaitUntilAdd(ctx, currBlockSize, nil); err != nil {
			return 0, err
		}
		buff := slicePool.RentSlice(currBlockSize)

		f := func(buff []byte, blockNum uint16 , curBlockSize, offset int64) func() {
			return func() {
				downloadBlock(buff, blockNum, curBlockSize, offset)
			}
		}(buff, blockNum, currBlockSize, offset)

		// send
		operationChannel <- f
	}

	// Wait for all chunks to be done.
	wg.Wait()
	if err != nil {
		return 0, err
	}

	return count, nil
}

func main() {

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	outputFile := os.Args[2]
	blobURL := os.Args[1]

	s := NewMultiSizeSlicePool(MaxBlockBlobBlockSize)
	c   := NewCacheLimiter(4 * 1024 * 1024 * 1024) // 4 GiB
	o := make(chan func(), 64)

	worker := func () {
		for f := range o {
			f()
		}
	}

	for i := 0; i < 64; i++ {
		go worker()
	}

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

	_, err = DownloadFile(ctx, fo, 8 * 1024 * 1024, b, s, c, o)
	if err != nil {
		fmt.Printf("Failed: %v\n", err)
		return
	}

	fmt.Println("Success")
}