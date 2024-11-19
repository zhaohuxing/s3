package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	. "github.com/zhaohuxing/s3/cmd"
)

var testdir = "./testdata"

func cpath(bucket, object string) string {
	return fmt.Sprintf("%s/%s", testdir, object)
}

func npath(bucket, prefixDir, name string) string {
	return fmt.Sprintf("%s/%s%s", testdir, prefixDir, name)
}

func getObjectInfo(ctx context.Context, bucket, object string, info *ObjectInfo) (obj ObjectInfo, err error) {
	//var eno syscall.Errno
	if info == nil {
		fi, eno := os.Stat(cpath(bucket, object))
		if eno == nil {
			size := fi.Size()
			if fi.IsDir() {
				size = 0
			}
			info = &ObjectInfo{
				Bucket:  bucket,
				ModTime: fi.ModTime(),
				Size:    size,
				IsDir:   fi.IsDir(),
			}
		}

		// replace links to external file systems with empty files
		if errors.Is(eno, syscall.ENOTSUP) {
			now := time.Now()
			info = &ObjectInfo{
				Bucket:  bucket,
				ModTime: now,
				Size:    0,
				IsDir:   false,
			}
			eno = nil
		}
	}

	if info == nil {
		return obj, errors.New("object is nil")
	}
	info.Name = object
	return *info, nil
}

func listDirFactory() ListDirFunc {
	return func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []*Entry, delayIsLeaf bool) {
		f, eno := os.Open(cpath(bucket, prefixDir))
		if eno != nil {
			return false, nil, false
		}
		defer f.Close()
		/*
			if fi, _ := f.Stat(); fi.(*fs.FileInfo).Atime() == 0 && prefixEntry == "" {
				entries = append(entries, &minio.Entry{Name: ""})
			}
		*/
		fis, eno := f.Readdir(0)
		if eno != nil {
			return
		}
		root := cpath(bucket, prefixDir) == "/"
		for _, fi := range fis {
			if root && (fi.Name() == ".sys") {
				continue
			} /*
				if stat, ok := fi.(*fs.FileStat); ok && stat.IsSymlink() {
					p := npath(bucket, prefixDir, fi.Name())
					if _, err := os.Stat(p); err != nil {
						fmt.Printf("stat %s: %s", p, err)
						continue
					}
				}*/
			entry := &Entry{Name: fi.Name(),
				Info: &ObjectInfo{
					Bucket:  bucket,
					Name:    fi.Name(),
					ModTime: fi.ModTime(),
					Size:    fi.Size(),
					IsDir:   fi.IsDir(),
				},
			}

			if fi.IsDir() {
				entry.Name += "/"
				entry.Info.Size = 0
			}
			entries = append(entries, entry)
		}
		if len(entries) == 0 {
			return true, nil, false
		}
		entries, delayIsLeaf = FilterListEntries(bucket, prefixDir, entries, prefixEntry, isLeaf)
		return false, entries, delayIsLeaf
	}
}

func isLeaf(bucket, leafPath string) bool {
	return !strings.HasSuffix(leafPath, "/")
}

func isLeafDir(bucket, object string) bool {
	f, eno := os.Open(cpath(bucket, object))
	if eno != nil {
		return false
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return false
	}
	return len(fis) == 0
}

func genFiles(root string) {

	// ./testdata
	// ./testdata/a1/b1/c1

	vals := strings.Split(root, "/")
	if len(vals) > 4 {
		for k := 1; k < 3; k++ {
			name := fmt.Sprintf("%d.txt", k)
			os.Create(root + "/" + name)
		}
		return
	}

	// 1. head files
	for i := 1; i < 3; i++ {
		name := fmt.Sprintf("a%d.txt", i)
		os.Create(root + "/" + name)
	}
	// 2. middle dirs
	for i := 'a'; i < 'd'; i++ {
		for j := 1; j < 4; j++ {
			dir := fmt.Sprintf("%c%d", i, j)
			os.Mkdir(root+"/"+dir, 0755)
			genFiles(root + "/" + dir)
			for k := 1; k < 3; k++ {
				name := fmt.Sprintf("%d.txt", k)
				os.Create(root + "/" + dir + name)
			}
		}
	}
	// 3. tail files
	for i := 1; i < 3; i++ {
		name := fmt.Sprintf("z%d.txt", i)
		os.Create(root + "/" + name)
	}
}

func ListObjectFn(prefix, marker, delimiter string, maxKeys int) {
	listpool := NewTreeWalkPool(time.Minute * 30)

	count := 0
	for {
		result, err := ListObjects(
			context.Background(), "", prefix, marker, delimiter, maxKeys,
			listpool, listDirFactory(), isLeaf, isLeafDir, getObjectInfo, getObjectInfo,
		)
		if err != nil {
			fmt.Println(err)
		}

		fmt.Printf("*****************(objects: %d)****************\n", len(result.Objects))
		for _, obj := range result.Objects {
			fmt.Println(obj.Name)
		}
		fmt.Println("*********************************************")
		if delimiter != "" {
			fmt.Printf("*****************(prefixs: %d)****************\n", len(result.Prefixes))
			for _, prefix := range result.Prefixes {
				fmt.Println(prefix)
			}
			fmt.Println("*********************************************")
		}
		count += len(result.Objects)
		marker = result.NextMarker
		if marker == "" {
			break
		}

	}
	fmt.Println("object count: ", count)
}

func TestGen(t *testing.T) {
	//genFiles("./testdata")
}

// case 1: delimiter="", prefix="", marker=<$nextMarker>, maxKeys=100
func TestLsCase1(t *testing.T) {
	delimiter, prefix, marker, maxKeys := "", "", "", 100
	ListObjectFn(prefix, marker, delimiter, maxKeys)
}

// case 2: delimiter="", prefix="<$prefix>", marker=<$nextMarker>, maxKeys=100
func TestLsCase2(t *testing.T) {
	delimiter, prefix, marker, maxKeys := "", "a", "", 100
	ListObjectFn(prefix, marker, delimiter, maxKeys)

	fmt.Println("====================")
	prefix = "a1/"
	ListObjectFn(prefix, marker, delimiter, maxKeys)

	fmt.Println("====================")
	prefix = "a1/c"
	ListObjectFn(prefix, marker, delimiter, maxKeys)
}

// case 3: delimiter="/", prefix="<$pfefix>", marker="<$nextMarker>, maxKeys=100"
func TestLsCase3(t *testing.T) {
	delimiter, prefix, marker, maxKeys := "/", "a", "", 100
	ListObjectFn(prefix, marker, delimiter, maxKeys)
}
