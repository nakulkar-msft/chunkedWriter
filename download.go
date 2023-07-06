package main

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
)

func DownloadFile(ctx context.Context,
	bb *blockblob.Client,
	filepath string,
	blockSize int64,
	s ByteSlicePooler,
	c CacheLimiter,
	o chan<- func()) (int64, error) {

	b := bb.BlobClient()

	// 1. Calculate the size of the destination file
	var size int64
	props, err := b.GetProperties(ctx, nil)
	if err != nil {
		return 0, err
	}
	size = *props.ContentLength

	// Because we'll write the file serially, open in APPEND mode
	file, err := os.OpenFile(filepath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

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
	file *os.File,
	fileSize int64,
	blockSize int64,
	slicePool ByteSlicePooler,
	cacheLimiter CacheLimiter,
	operationChannel chan<- func()) (int64, error) {

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

	// to synchronize all block scheduling and writer threads.
	var wg sync.WaitGroup

	// file serial writer
	count := fileSize
	numBlocks := uint16(((count - 1) / blockSize) + 1)

	blocks := make([]chan []byte, numBlocks)
	for i := range blocks {
		blocks[i] = make(chan []byte)
	}

	totalWrite := int64(0)
	go func() {
		for _, block := range blocks {
			select {
			case <-ctx.Done():
				return
			case buff := <-block:
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

		n, err := b.DownloadBuffer(ctx, buff, &blob.DownloadBufferOptions{
			Concurrency: 1,
			BlockSize:   blockSize,
			Range:       blob.HTTPRange{Offset: offset, Count: currentBlockSize},
		})

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

	var err error
	go func() {
		// This goroutine will monitor above channel and
		// cancel the context if any block reports error
		err = <-errorChannel
		cancel()
		return
	}()

	for blockNum := uint16(0); blockNum < numBlocks; blockNum++ {
		currBlockSize := blockSize
		if blockNum == numBlocks-1 { // Last block
			// Remove size of all transferred blocks from total
			currBlockSize = count - (int64(blockNum) * blockSize)
		}

		offset := int64(blockNum) * blockSize

		// allocate a buffer. This buffer will be released by the fileWriter
		if err := cacheLimiter.WaitUntilAdd(ctx, currBlockSize, nil); err != nil {
			postError(err)
			break
		}
		buff := slicePool.RentSlice(currBlockSize)

		f := func(buff []byte, blockNum uint16, curBlockSize, offset int64) func() {
			return func() {
				downloadBlock(buff, blockNum, curBlockSize, offset)
			}
		}(buff, blockNum, currBlockSize, offset)

		// send
		wg.Add(1)
		operationChannel <- f
	}

	// Wait for all chunks to be done.
	wg.Wait()
	if err != nil {
		return 0, err
	}

	return count, nil
}