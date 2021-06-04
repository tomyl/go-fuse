// Copyright 2019 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fs

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/hanwen/go-fuse/v2/internal/testutil"
)

func testMount(t *testing.T, root InodeEmbedder, opts *Options) (string, *fuse.Server, func()) {
	t.Helper()

	mntDir := testutil.TempDir()
	if opts == nil {
		opts = &Options{
			FirstAutomaticIno: 1,
		}
	}
	opts.Debug = testutil.VerboseTest()

	server, err := Mount(mntDir, root, opts)
	if err != nil {
		t.Fatal(err)
	}
	return mntDir, server, func() {
		if err := server.Unmount(); err != nil {
			t.Fatalf("testMount: Unmount failed: %v", err)
		}
		if err := syscall.Rmdir(mntDir); err != nil {
			t.Errorf("testMount: Remove failed: %v", err)
		}
	}
}

func TestDefaultOwner(t *testing.T) {
	want := "hello"
	root := &Inode{}
	mntDir, _, clean := testMount(t, root, &Options{
		FirstAutomaticIno: 1,
		OnAdd: func(ctx context.Context) {
			n := root.EmbeddedInode()
			ch := n.NewPersistentInode(
				ctx,
				&MemRegularFile{
					Data: []byte(want),
				},
				StableAttr{})
			n.AddChild("file", ch, false)
		},
		UID: 42,
		GID: 43,
	})
	defer clean()

	var st syscall.Stat_t
	if err := syscall.Lstat(mntDir+"/file", &st); err != nil {
		t.Fatalf("Lstat: %v", err)
	} else if st.Uid != 42 || st.Gid != 43 {
		t.Fatalf("Got Lstat %d, %d want 42,43", st.Uid, st.Gid)
	}
}

func TestDataFile(t *testing.T) {
	want := "hello"
	root := &Inode{}
	mntDir, _, clean := testMount(t, root, &Options{
		FirstAutomaticIno: 1,
		OnAdd: func(ctx context.Context) {
			n := root.EmbeddedInode()
			ch := n.NewPersistentInode(
				ctx,
				&MemRegularFile{
					Data: []byte(want),
					Attr: fuse.Attr{
						Mode: 0464,
					},
				},
				StableAttr{})
			n.AddChild("file", ch, false)
		},
	})
	defer clean()

	var st syscall.Stat_t
	if err := syscall.Lstat(mntDir+"/file", &st); err != nil {
		t.Fatalf("Lstat: %v", err)
	}

	if want := uint32(syscall.S_IFREG | 0464); st.Mode != want {
		t.Errorf("got mode %o, want %o", st.Mode, want)
	}

	if st.Size != int64(len(want)) || st.Blocks != 8 || st.Blksize != 4096 {
		t.Errorf("got %#v, want sz = %d, 8 blocks, 4096 blocksize", st, len(want))
	}

	fd, err := syscall.Open(mntDir+"/file", syscall.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var buf [1024]byte
	n, err := syscall.Read(fd, buf[:])
	if err != nil {
		t.Errorf("Read: %v", err)
	}

	if err := syscall.Close(fd); err != nil {
		t.Errorf("Close: %v", err)
	}

	got := string(buf[:n])
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}

	replace := []byte("replaced!")
	if err := ioutil.WriteFile(mntDir+"/file", replace, 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if gotBytes, err := ioutil.ReadFile(mntDir + "/file"); err != nil {
		t.Fatalf("ReadFile: %v", err)
	} else if bytes.Compare(replace, gotBytes) != 0 {
		t.Fatalf("read: got %q want %q", gotBytes, replace)
	}
}

func TestDataFileLargeRead(t *testing.T) {
	root := &Inode{}

	data := make([]byte, 256*1024)
	rand.Read(data[:])
	mntDir, _, clean := testMount(t, root, &Options{
		FirstAutomaticIno: 1,
		OnAdd: func(ctx context.Context) {
			n := root.EmbeddedInode()
			ch := n.NewPersistentInode(
				ctx,
				&MemRegularFile{
					Data: data,
					Attr: fuse.Attr{
						Mode: 0464,
					},
				},
				StableAttr{})
			n.AddChild("file", ch, false)
		},
	})
	defer clean()
	got, err := ioutil.ReadFile(mntDir + "/file")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Errorf("roundtrip read had change")
	}
}

type SymlinkerRoot struct {
	Inode
}

func (s *SymlinkerRoot) Symlink(ctx context.Context, target, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	l := &MemSymlink{
		Data: []byte(target),
	}

	ch := s.NewPersistentInode(ctx, l, StableAttr{Mode: syscall.S_IFLNK})
	return ch, 0
}

func TestDataSymlink(t *testing.T) {
	root := &SymlinkerRoot{}

	mntDir, _, clean := testMount(t, root, nil)
	defer clean()

	if err := syscall.Symlink("target", mntDir+"/link"); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	if got, err := os.Readlink(mntDir + "/link"); err != nil {
		t.Fatalf("Readlink: %v", err)
	} else if want := "target"; got != want {
		t.Errorf("Readlink: got %q want %q", got, want)
	}
}

func TestRenameAtomic2(t *testing.T) {
	opts := &Options{}
	opts.Debug = testutil.VerboseTest()
	mountPoint := testutil.TempDir()
	defer os.RemoveAll(mountPoint)
	fs := &dummyRoot{
		source: &dummyNode{data: []byte("source data")},
		dest:   &dummyNode{data: []byte("dest data")},
	}
	server, err := Mount(mountPoint, fs, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Unmount()

	srcPath := filepath.Join(mountPoint, "src")
	dstPath := filepath.Join(mountPoint, "dst")

	srcData := []byte("source data")
	dstData := []byte("dest data")

	{
		data, err := ioutil.ReadFile(srcPath)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if string(data) != string(srcData) {
			t.Fatalf("ReadFile: unexpected data %q", data)
		}
	}

	{
		data, err := ioutil.ReadFile(dstPath)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		if string(data) != string(dstData) {
			t.Fatalf("ReadFile: unexpected data %q", data)
		}
	}

	t.Run("Loops", func(t *testing.T) {
		var running int32 = 1
		maxRenames := 100

		t.Run("Read", func(t *testing.T) {
			t.Parallel()

			defer func() {
				// Signal the Rename goroutine to stop in case this subtest fails
				atomic.StoreInt32(&running, 0)
			}()

			for atomic.LoadInt32(&running) == 1 {
				data, err := ioutil.ReadFile(dstPath)
				if err != nil {
					t.Fatalf("ReadFile: %v", err)
				}
				if string(data) != string(srcData) && string(data) != string(dstData) {
					t.Fatalf("bad data (len %d) in destination file: %q", len(data), string(data))
				}
			}
		})

		t.Run("Rename", func(t *testing.T) {
			t.Parallel()

			defer func() {
				// Signal the Read goroutine to stop when loop is done
				atomic.StoreInt32(&running, 0)
			}()

			for i := 0; i < maxRenames && atomic.LoadInt32(&running) == 1; i++ {
				// Rename src to dst
				if err := os.Rename(srcPath, dstPath); err != nil {
					t.Fatalf("Rename: %v", err)
				}
			}
		})
	})
}

type dummyRoot struct {
	Inode

	source *dummyNode
	dest   *dummyNode
}

var _ = (NodeLookuper)((*dummyRoot)(nil))

func (n *dummyRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*Inode, syscall.Errno) {
	sattr := StableAttr{Mode: fuse.S_IFREG}
	if name == "src" {
		return n.NewInode(ctx, n.source, sattr), OK
	}
	if name == "dst" {
		return n.NewInode(ctx, n.dest, sattr), OK
	}
	return nil, syscall.ENOENT
}

var _ = (NodeRenamer)((*dummyRoot)(nil))

var dummyLock sync.Mutex

func (n *dummyRoot) Rename(ctx context.Context, name string, newParent InodeEmbedder, newName string, flags uint32) syscall.Errno {
	dummyLock.Lock()
	defer dummyLock.Unlock()

	if name == "src" && newName == "dst" {
		n.dest.data = n.source.data
		return OK
	}
	return syscall.ENOTSUP
}

type dummyNode struct {
	Inode

	data []byte
}

var _ = (NodeGetattrer)((*dummyNode)(nil))

func (n *dummyNode) Getattr(ctx context.Context, f FileHandle, out *fuse.AttrOut) syscall.Errno {
	dummyLock.Lock()
	defer dummyLock.Unlock()

	out.Size = uint64(len(n.data))
	return OK
}

var _ = (NodeOpener)((*dummyNode)(nil))

func (n *dummyNode) Open(ctx context.Context, flags uint32) (fh FileHandle, fuseFlags uint32, errno syscall.Errno) {
	dummyLock.Lock()
	defer dummyLock.Unlock()
	return &dummyFile{reader: bytes.NewReader(n.data)}, fuse.FOPEN_DIRECT_IO, OK
}

type dummyFile struct {
	reader *bytes.Reader
}

var _ = (FileReader)((*dummyFile)(nil))

func (n *dummyFile) Read(ctx context.Context, dest []byte, offset int64) (fuse.ReadResult, syscall.Errno) {
	nr, err := n.reader.ReadAt(dest, offset)
	buf := dest[:nr]
	r := fuse.ReadResultData(buf)
	if err != nil && err != io.EOF {
		return r, ToErrno(err)
	}
	return r, OK
}

var _ = (FileFlusher)((*dummyFile)(nil))

func (n *dummyFile) Flush(ctx context.Context) syscall.Errno {
	return OK
}

var _ = (FileReleaser)((*dummyFile)(nil))

func (n *dummyFile) Release(ctx context.Context) syscall.Errno {
	return OK
}
