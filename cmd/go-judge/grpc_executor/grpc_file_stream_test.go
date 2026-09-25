package grpcexecutor

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/criyle/go-judge/envexec"
	"github.com/criyle/go-judge/filestore"
	"github.com/criyle/go-judge/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fileAddStreamStub struct {
	grpc.ServerStream
	chunks  []*pb.FileContent
	recvErr error
	sendErr error
	pos     int
	result  *pb.FileID
}

func (s *fileAddStreamStub) Recv() (*pb.FileContent, error) {
	if s.pos < len(s.chunks) {
		chunk := s.chunks[s.pos]
		s.pos++
		return chunk, nil
	}
	if s.recvErr != nil {
		err := s.recvErr
		s.recvErr = nil
		return nil, err
	}
	return nil, io.EOF
}

func (s *fileAddStreamStub) SendAndClose(result *pb.FileID) error {
	s.result = result
	return s.sendErr
}

func newFileAddStreamTestServer(t *testing.T) (*execServer, filestore.FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	fs := filestore.NewFileLocalStore(dir)
	return &execServer{fs: fs}, fs, dir
}

func assertStoreDirEmpty(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no files in store, found %d", len(entries))
	}
}

func TestFileAddStream(t *testing.T) {
	server, fs, _ := newFileAddStreamTestServer(t)
	stream := &fileAddStreamStub{chunks: []*pb.FileContent{
		pb.FileContent_builder{Name: "artifact.bin", Content: []byte("hello ")}.Build(),
		pb.FileContent_builder{Content: []byte("world")}.Build(),
		pb.FileContent_builder{Name: "artifact.bin", Content: []byte("!")}.Build(),
	}}

	if err := server.FileAddStream(stream); err != nil {
		t.Fatal(err)
	}
	if stream.result == nil || stream.result.GetFileID() == "" {
		t.Fatal("expected a file ID")
	}

	name, file := fs.Get(stream.result.GetFileID())
	if file == nil {
		t.Fatal("uploaded file not found in store")
	}
	if name != "artifact.bin" {
		t.Fatalf("expected artifact.bin, got %q", name)
	}
	r, err := envexec.FileToReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(content), "hello world!"; got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestFileAddStreamRejectsEmptyStream(t *testing.T) {
	server, _, dir := newFileAddStreamTestServer(t)
	stream := &fileAddStreamStub{}

	err := server.FileAddStream(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
	assertStoreDirEmpty(t, dir)
}

func TestFileAddStreamRejectsNameChange(t *testing.T) {
	server, _, dir := newFileAddStreamTestServer(t)
	stream := &fileAddStreamStub{chunks: []*pb.FileContent{
		pb.FileContent_builder{Name: "a.bin", Content: []byte("a")}.Build(),
		pb.FileContent_builder{Name: "b.bin", Content: []byte("b")}.Build(),
	}}

	err := server.FileAddStream(stream)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
	assertStoreDirEmpty(t, dir)
}

func TestFileAddStreamEnforcesSizeLimit(t *testing.T) {
	server, _, dir := newFileAddStreamTestServer(t)
	server.fileUploadLimit = 4
	stream := &fileAddStreamStub{chunks: []*pb.FileContent{
		pb.FileContent_builder{Name: "artifact.bin", Content: []byte("123")}.Build(),
		pb.FileContent_builder{Content: []byte("45")}.Build(),
	}}

	err := server.FileAddStream(stream)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("expected ResourceExhausted, got %v", err)
	}
	assertStoreDirEmpty(t, dir)
}

func TestFileAddStreamCleansUpOnReceiveError(t *testing.T) {
	server, _, dir := newFileAddStreamTestServer(t)
	stream := &fileAddStreamStub{
		chunks: []*pb.FileContent{
			pb.FileContent_builder{Name: "artifact.bin", Content: []byte("partial")}.Build(),
		},
		recvErr: context.Canceled,
	}

	err := server.FileAddStream(stream)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	assertStoreDirEmpty(t, dir)
}

func TestFileAddStreamRollsBackOnSendFailure(t *testing.T) {
	server, fs, dir := newFileAddStreamTestServer(t)
	sendErr := errors.New("send failed")
	stream := &fileAddStreamStub{
		chunks: []*pb.FileContent{
			pb.FileContent_builder{Name: "artifact.bin", Content: []byte("complete")}.Build(),
		},
		sendErr: sendErr,
	}

	err := server.FileAddStream(stream)
	if !errors.Is(err, sendErr) {
		t.Fatalf("expected send failure, got %v", err)
	}
	if got := len(fs.List()); got != 0 {
		t.Fatalf("expected no published files, found %d", got)
	}
	assertStoreDirEmpty(t, dir)
}
