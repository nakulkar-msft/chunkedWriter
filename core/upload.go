// Copyright Microsoft <wastore@microsoft.com>
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
package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/google/uuid"
)

type nopCloser struct {
	io.ReadSeeker
}

func (nopCloser) Close() error { return nil }

func withNopCloser(r io.ReadSeeker) io.ReadSeekCloser {
	return nopCloser{r}
}

func (c *copier) UploadFile(ctx context.Context,
	                        b *blockblob.Client,
                            filepath string,
                            blockSize int64) error {

	// 1. Calculate the size of the destination file
	file, err := os.Open(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	fileSize := stat.Size()

	if fileSize <= blockSize { //perform a single thread copy here.
		_, err := b.Upload(ctx, newPacedReadSeekCloser(ctx, c.pacer, file), &blockblob.UploadOptions{})
		return err
	}

	return c.uploadInternal(ctx, b, file, fileSize, blockSize)
}

func (c *copier) uploadInternal(ctx context.Context,
                                b *blockblob.Client,
                                file io.ReadSeekCloser,
                                fileSize int64,
                                blockSize int64) error {


	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// short hand for routines to report and error
	errorChannel := make(chan error)
	postError := func(err error) {
		select {
		case <-ctx.Done():
		case errorChannel <- err:
		}
	}

	numBlocks := uint16(((fileSize - 1) / blockSize) + 1)
	var wg sync.WaitGroup

	blockNames := make([]string, numBlocks)

	uploadBlock := func(buff []byte, blockIndex uint16) {
		defer wg.Done()
		body := newPacedReadSeekCloser(ctx, c.pacer, withNopCloser(bytes.NewReader(buff)))
		blockName := base64.StdEncoding.EncodeToString([]byte(uuid.New().String()))
		blockNames[blockIndex] = blockName

		_, err := b.StageBlock(ctx, blockNames[blockIndex], body, &blockblob.StageBlockOptions{})
		if err != nil {
			postError(err)
		}
	}

	var err error
	go func() {
		// This goroutine will monitor error channel and
		// cancel the context if any block reports error
		err = <-errorChannel
		cancel()
		return
	}()

	for blockNum := uint16(0); blockNum < numBlocks; blockNum++ {
		currBlockSize := blockSize
		if blockNum == numBlocks-1 { // Last block
			// Remove size of all transferred blocks from total
			currBlockSize = fileSize - (int64(blockNum) * blockSize)
		}

		if err := c.cacheLimiter.WaitUntilAdd(ctx, currBlockSize, nil); err != nil {
			postError(err)
			break
		}
		buff := c.slicePool.RentSlice(currBlockSize)

		n, err := file.Read(buff)
		if err != nil {
			postError(err)
			break
		}
		if n != int(currBlockSize) {
			postError(errors.New("invalid read"))
			break
		}

		f := func(buff []byte, blockNum uint16) func() {
			return func() { uploadBlock(buff, blockNum) }
		}(buff, blockNum)

		wg.Add(1)
		c.opChan <- f
	}

	// Wait for all chunks to be done.
	wg.Wait()
	if err != nil {
		return err
	}

	_, err = b.CommitBlockList(ctx, blockNames, &blockblob.CommitBlockListOptions{})

	return err
}
