package cmd

import (
	"context"
	"os"
	"strings"
	"syscall"

	errgroup "github.com/zhaohuxing/s3/pkg/sync"
)

func listObjectsNonSlash(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int, tpool *TreeWalkPool, listDir ListDirFunc, isLeaf IsLeafFunc, isLeafDir IsLeafDirFunc, getObjInfo func(context.Context, string, string, *ObjectInfo) (ObjectInfo, error), getObjectInfoDirs ...func(context.Context, string, string, *ObjectInfo) (ObjectInfo, error)) (loi ListObjectsInfo, err error) {
	endWalkCh := make(chan struct{})
	defer close(endWalkCh)
	recursive := true
	walkResultCh := startTreeWalk(ctx, bucket, prefix, "", recursive, listDir, isLeaf, isLeafDir, endWalkCh)

	var objInfos []ObjectInfo
	var eof bool
	var prevPrefix string

	for {
		if len(objInfos) == maxKeys {
			break
		}
		result, ok := <-walkResultCh
		if !ok {
			eof = true
			break
		}

		var objInfo ObjectInfo
		var err error

		index := strings.Index(strings.TrimPrefix(result.entry.Name, prefix), delimiter)
		if index == -1 {
			objInfo, err = getObjInfo(ctx, bucket, result.entry.Name, result.entry.Info)
			if err != nil {
				// Ignore errFileNotFound as the object might have got
				// deleted in the interim period of listing and getObjectInfo(),
				// ignore quorum error as it might be an entry from an outdated disk.
				if err == syscall.ENOENT || os.IsNotExist(err) {
					continue
				}
				return loi, err
			}
		} else {
			index = len(prefix) + index + len(delimiter)
			currPrefix := result.entry.Name[:index]
			if currPrefix == prevPrefix {
				continue
			}
			prevPrefix = currPrefix

			objInfo = ObjectInfo{
				Bucket: bucket,
				Name:   currPrefix,
				IsDir:  true,
			}
		}

		if objInfo.Name <= marker {
			continue
		}

		objInfos = append(objInfos, objInfo)
		if result.end {
			eof = true
			break
		}
	}

	result := ListObjectsInfo{}
	for _, objInfo := range objInfos {
		if objInfo.IsDir {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	if !eof {
		result.IsTruncated = true
		if len(objInfos) > 0 {
			result.NextMarker = objInfos[len(objInfos)-1].Name
		}
	}

	return result, nil
}

func listObjects(
	ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int,
	tpool *TreeWalkPool,
	listDir ListDirFunc, isLeaf IsLeafFunc, isLeafDir IsLeafDirFunc,
	getObjInfo func(context.Context, string, string, *ObjectInfo) (ObjectInfo, error),
	getObjectInfoDirs ...func(context.Context, string, string, *ObjectInfo,
	) (ObjectInfo, error)) (loi ListObjectsInfo, err error) {
	if delimiter != SlashSeparator && delimiter != "" {
		return listObjectsNonSlash(ctx, bucket, prefix, marker, delimiter, maxKeys, tpool, listDir, isLeaf, isLeafDir, getObjInfo, getObjectInfoDirs...)
	}

	// Marker is set validate pre-condition.
	if marker != "" {
		// Marker not common with prefix is not implemented. Send an empty response
		if !HasPrefix(marker, prefix) {
			return loi, nil
		}
	}

	// With max keys of zero we have reached eof, return right here.
	if maxKeys == 0 {
		return loi, nil
	}

	// For delimiter and prefix as '/' we do not list anything at all
	// since according to s3 spec we stop at the 'delimiter'
	// along // with the prefix. On a flat namespace with 'prefix'
	// as '/' we don't have any entries, since all the keys are
	// of form 'keyName/...'
	if delimiter == SlashSeparator && prefix == SlashSeparator {
		return loi, nil
	}

	// Over flowing count - reset to maxObjectList.
	if maxKeys < 0 || maxKeys > maxObjectList {
		maxKeys = maxObjectList
	}

	// Default is recursive, if delimiter is set then list non recursive.
	recursive := true
	if delimiter == SlashSeparator {
		recursive = false
	}

	walkResultCh, endWalkCh := tpool.Release(listParams{bucket, recursive, marker, prefix})
	if walkResultCh == nil {
		endWalkCh = make(chan struct{})
		walkResultCh = startTreeWalk(ctx, bucket, prefix, marker, recursive, listDir, isLeaf, isLeafDir, endWalkCh)
	}

	var eof bool
	var nextMarker string

	// List until maxKeys requested.
	g := errgroup.WithNErrs(maxKeys).WithConcurrency(10)
	ctx, cancel := g.WithCancelOnError(ctx)
	defer cancel()

	objInfoFound := make([]*ObjectInfo, maxKeys)
	var i int
	for i = 0; i < maxKeys; i++ {
		i := i
		walkResult, ok := <-walkResultCh
		if !ok {
			// Closed channel.
			eof = true
			break
		}

		if HasSuffix(walkResult.entry.Name, SlashSeparator) {
			g.Go(func() error {
				for _, getObjectInfoDir := range getObjectInfoDirs {
					objInfo, err := getObjectInfoDir(ctx, bucket, walkResult.entry.Name, walkResult.entry.Info)
					if err == nil {
						objInfoFound[i] = &objInfo
						// Done...
						return nil
					}

					// Add temp, may be overridden,
					if err == syscall.ENOENT || os.IsNotExist(err) {
						objInfoFound[i] = &ObjectInfo{
							Bucket: bucket,
							Name:   walkResult.entry.Name,
							IsDir:  true,
						}
						continue
					}
					return err
				}
				return nil
			}, i)
		} else {
			g.Go(func() error {
				objInfo, err := getObjInfo(ctx, bucket, walkResult.entry.Name, walkResult.entry.Info)
				if err != nil {
					// Ignore errFileNotFound as the object might have got
					// deleted in the interim period of listing and getObjectInfo(),
					// ignore quorum error as it might be an entry from an outdated disk.
					if err == syscall.ENOENT || os.IsNotExist(err) {
						return nil
					}
					return err
				}
				objInfoFound[i] = &objInfo
				return nil
			}, i)
		}

		if walkResult.end {
			eof = true
			break
		}
	}
	if err := g.WaitErr(); err != nil {
		return loi, err
	}
	// Copy found objects
	objInfos := make([]ObjectInfo, 0, i+1)
	for _, objInfo := range objInfoFound {
		if objInfo == nil {
			continue
		}
		objInfos = append(objInfos, *objInfo)
		nextMarker = objInfo.Name
	}

	// Save list routine for the next marker if we haven't reached EOF.
	params := listParams{bucket, recursive, nextMarker, prefix}
	if !eof {
		tpool.Set(params, walkResultCh, endWalkCh)
	}

	result := ListObjectsInfo{}
	for _, objInfo := range objInfos {
		if objInfo.IsDir && delimiter == SlashSeparator && objInfo.Name != prefix {
			result.Prefixes = append(result.Prefixes, objInfo.Name)
			continue
		}
		result.Objects = append(result.Objects, objInfo)
	}

	if !eof {
		result.IsTruncated = true
		if len(objInfos) > 0 {
			result.NextMarker = objInfos[len(objInfos)-1].Name
		}
	}

	// Success.
	return result, nil
}
