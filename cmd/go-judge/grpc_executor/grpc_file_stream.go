package grpcexecutor

import (
	"io"
	"os"

	"github.com/criyle/go-judge/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FileAddStream stores a client-streamed file in the existing file store.
// The first message establishes the file name; later messages may omit it or
// repeat the same name. The file is only published after the stream completes.
func (e *execServer) FileAddStream(stream pb.Executor_FileAddStreamServer) error {
	first, err := stream.Recv()
	if err == io.EOF {
		return status.Error(codes.InvalidArgument, "file upload stream is empty")
	}
	if err != nil {
		return err
	}

	name := first.GetName()
	f, err := e.fs.New()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	path := f.Name()
	closed := false
	committed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
		if !committed {
			_ = os.Remove(path)
		}
	}()

	var total uint64
	writeChunk := func(content []byte) error {
		total += uint64(len(content))
		if e.fileUploadLimit > 0 && total > uint64(e.fileUploadLimit) {
			return status.Errorf(codes.ResourceExhausted, "file upload exceeds limit: %d > %d bytes", total, e.fileUploadLimit)
		}
		if len(content) == 0 {
			return nil
		}
		n, err := f.Write(content)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if n != len(content) {
			return status.Error(codes.Internal, io.ErrShortWrite.Error())
		}
		return nil
	}

	if err := writeChunk(first.GetContent()); err != nil {
		return err
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if chunkName := chunk.GetName(); chunkName != "" && chunkName != name {
			return status.Errorf(codes.InvalidArgument, "file name changed during upload: %q -> %q", name, chunkName)
		}
		if err := writeChunk(chunk.GetContent()); err != nil {
			return err
		}
	}

	if err := f.Close(); err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	closed = true

	fid, err := e.fs.Add(name, path)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if err := stream.SendAndClose(pb.FileID_builder{FileID: fid}.Build()); err != nil {
		e.fs.Remove(fid)
		return err
	}
	committed = true
	return nil
}
