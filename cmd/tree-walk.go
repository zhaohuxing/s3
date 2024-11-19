package cmd

import (
	"context"
	"runtime"
	"sort"
	"strings"
)

type Entry struct {
	Name string
	Info *ObjectInfo
}

// TreeWalkResult - Tree walk result carries results of tree walking.
type TreeWalkResult struct {
	entry      *Entry
	isEmptyDir bool
	end        bool
}

// Return entries that have prefix prefixEntry.
// The supplied entries are modified and the returned string is a subslice of entries.
func filterMatchingPrefix(entries []*Entry, prefixEntry string) []*Entry {
	if len(entries) == 0 || prefixEntry == "" {
		return entries
	}
	// Write to the beginning of entries.
	dst := entries[:0]
	for _, s := range entries {
		if !HasPrefix(s.Name, prefixEntry) {
			continue
		}
		dst = append(dst, s)
	}
	return dst
}

// ListDirFunc - "listDir" function of type listDirFunc returned by listDirFactory() - explained below.
type ListDirFunc func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []*Entry, delayIsLeaf bool)

// IsLeafFunc - A function isLeaf of type isLeafFunc is used to detect if an
// entry is a leaf entry. There are 2 scenarios where isLeaf should behave
// differently depending on the backend:
//  1. FS backend object listing - isLeaf is true if the entry
//     has no trailing "/"
//  2. Erasure backend object listing - isLeaf is true if the entry
//     is a directory and contains xl.meta
type IsLeafFunc func(string, string) bool

// IsLeafDirFunc - A function isLeafDir of type isLeafDirFunc is used to detect
// if an entry is empty directory.
type IsLeafDirFunc func(string, string) bool

func filterListEntries(bucket, prefixDir string, entries []*Entry, prefixEntry string, isLeaf IsLeafFunc) ([]*Entry, bool) {
	// Filter entries that have the prefix prefixEntry.
	entries = filterMatchingPrefix(entries, prefixEntry)

	// Listing needs to be sorted.
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})
	return entries, false
}

// treeWalk walks directory tree recursively pushing TreeWalkResult into the channel as and when it encounters files.
func doTreeWalk(ctx context.Context, bucket, prefixDir, entryPrefixMatch, marker string, recursive bool, listDir ListDirFunc, isLeaf IsLeafFunc, isLeafDir IsLeafDirFunc, resultCh chan TreeWalkResult, endWalkCh <-chan struct{}, isEnd bool) (emptyDir bool, treeErr error) {
	// Example:
	// if prefixDir="one/two/three/" and marker="four/five.txt" treeWalk is recursively
	// called with prefixDir="one/two/three/four/" and marker="five.txt"

	var markerBase, markerDir string
	if marker != "" {
		// Ex: if marker="four/five.txt", markerDir="four/" markerBase="five.txt"
		markerSplit := strings.SplitN(marker, SlashSeparator, 2)
		markerDir = markerSplit[0]
		if len(markerSplit) == 2 {
			markerDir += SlashSeparator
			markerBase = markerSplit[1]
		}
	}

	emptyDir, entries, delayIsLeaf := listDir(bucket, prefixDir, entryPrefixMatch)
	// When isleaf check is delayed, make sure that it is set correctly here.
	if delayIsLeaf && isLeaf == nil || isLeafDir == nil {
		return false, errInvalidArgument
	}

	// For an empty list return right here.
	if emptyDir {
		return true, nil
	}

	// example:
	// If markerDir="four/" Search() returns the index of "four/" in the sorted
	// entries list so we skip all the entries till "four/"
	idx := sort.Search(len(entries), func(i int) bool {
		return entries[i].Name >= markerDir
	})
	entries = entries[idx:]
	// For an empty list after search through the entries, return right here.
	if len(entries) == 0 {
		return false, nil
	}

	for i, entry := range entries {
		var leaf, leafDir bool
		if i == 0 && entry.Name == "" {
			select {
			case <-endWalkCh:
				return false, errWalkAbort
			case resultCh <- TreeWalkResult{entry: &Entry{prefixDir, entry.Info}, isEmptyDir: leafDir, end: (i == len(entries)-1) && isEnd}:
			}
			continue
		}

		leaf = !HasSuffix(entry.Name, slashSeparator)

		if HasSuffix(entry.Name, slashSeparator) {
			leafDir = isLeafDir(bucket, pathJoin(prefixDir, entry.Name))
		}

		isDir := !leafDir && !leaf

		if i == 0 && markerDir == entry.Name {
			if !recursive {
				// Skip as the marker would already be listed in the previous listing.
				continue
			}
			if recursive && !isDir {
				// We should not skip for recursive listing and if markerDir is a directory
				// for ex. if marker is "four/five.txt" markerDir will be "four/" which
				// should not be skipped, instead it will need to be treeWalk()'ed into.

				// Skip if it is a file though as it would be listed in previous listing.
				continue
			}
		}
		if recursive && isDir {
			// If the entry is a directory, we will need recurse into it.
			markerArg := ""
			if entry.Name == markerDir {
				// We need to pass "five.txt" as marker only if we are
				// recursing into "four/"
				markerArg = markerBase
			}
			prefixMatch := "" // Valid only for first level treeWalk and empty for subdirectories.
			// markIsEnd is passed to this entry's treeWalk() so that treeWalker.end can be marked
			// true at the end of the treeWalk stream.
			markIsEnd := i == len(entries)-1 && isEnd
			emptyDir, err := doTreeWalk(ctx, bucket, pathJoin(prefixDir, entry.Name), prefixMatch, markerArg, recursive,
				listDir, isLeaf, isLeafDir, resultCh, endWalkCh, markIsEnd)
			if err != nil {
				return false, err
			}

			// A nil totalFound means this is an empty directory that
			// needs to be sent to the result channel, otherwise continue
			// to the next entry.
			if !emptyDir {
				continue
			}
		}

		// EOF is set if we are at last entry and the caller indicated we at the end.
		isEOF := (i == len(entries)-1) && isEnd
		entry.Name = pathJoin(prefixDir, entry.Name)
		select {
		case <-endWalkCh:
			return false, errWalkAbort
		case resultCh <- TreeWalkResult{entry: entry, isEmptyDir: leafDir, end: isEOF}:
		}
	}

	// Everything is listed.
	return false, nil
}

// Initiate a new treeWalk in a goroutine.
func startTreeWalk(ctx context.Context, bucket, prefix, marker string, recursive bool, listDir ListDirFunc, isLeaf IsLeafFunc, isLeafDir IsLeafDirFunc, endWalkCh <-chan struct{}) chan TreeWalkResult {
	// Example 1
	// If prefix is "one/two/three/" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/three/" and marker="four/five.txt"
	// and entryPrefixMatch=""

	// Example 2
	// if prefix is "one/two/th" and marker is "one/two/three/four/five.txt"
	// treeWalk is called with prefixDir="one/two/" and marker="three/four/five.txt"
	// and entryPrefixMatch="th"

	resultCh := make(chan TreeWalkResult, maxObjectList)
	entryPrefixMatch := prefix
	prefixDir := ""
	lastIndex := strings.LastIndex(prefix, SlashSeparator)
	if lastIndex != -1 {
		entryPrefixMatch = prefix[lastIndex+1:]
		prefixDir = prefix[:lastIndex+1]
	}
	marker = strings.TrimPrefix(marker, prefixDir)
	go func() {
		isEnd := true // Indication to start walking the tree with end as true.
		doTreeWalk(ctx, bucket, prefixDir, entryPrefixMatch, marker, recursive, listDir, isLeaf, isLeafDir, resultCh, endWalkCh, isEnd)
		close(resultCh)
	}()
	return resultCh
}

var globalWindowsOSName = "windows"

// HasPrefix - Prefix matcher string matches prefix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func HasPrefix(s string, prefix string) bool {
	if runtime.GOOS == globalWindowsOSName {
		return strings.HasPrefix(strings.ToLower(s), strings.ToLower(prefix))
	}
	return strings.HasPrefix(s, prefix)
}

// HasSuffix - Suffix matcher string matches suffix in a platform specific way.
// For example on windows since its case insensitive we are supposed
// to do case insensitive checks.
func HasSuffix(s string, suffix string) bool {
	if runtime.GOOS == globalWindowsOSName {
		return strings.HasSuffix(strings.ToLower(s), strings.ToLower(suffix))
	}
	return strings.HasSuffix(s, suffix)
}
