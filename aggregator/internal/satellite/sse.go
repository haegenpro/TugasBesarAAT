package satellite

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

const UserAgent = "aat-m2-satellite/1.0"

type Event struct {
	ID   string
	Type string
	Data []byte
}

type Stream struct {
	r           *bufio.Reader
	maxLine     int
	frame       bytes.Buffer
	partialLine bool
}

var ErrLineTooLong = errors.New("server-sent event line exceeds the accepted size")
var ErrEventTooLong = errors.New("server-sent event exceeds the accepted size")

func NewStream(r io.Reader, maxLine int) *Stream {
	return &Stream{r: bufio.NewReaderSize(r, 64<<10), maxLine: maxLine}
}

func (s *Stream) Buffered() int { return s.r.Buffered() }

func (s *Stream) line() ([]byte, error) {
	var out []byte
	for {
		chunk, err := s.r.ReadSlice('\n')
		s.frame.Write(chunk)
		out = append(out, chunk...)
		s.partialLine = err == bufio.ErrBufferFull || (len(chunk) > 0 && chunk[len(chunk)-1] != '\n')
		if len(out) > s.maxLine {
			return nil, ErrLineTooLong
		}
		if s.frame.Len() > s.maxLine {
			return nil, ErrEventTooLong
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil && len(out) == 0 {
			return nil, err
		}
		return bytes.TrimSuffix(bytes.TrimSuffix(out, []byte{'\n'}), []byte{'\r'}), nil
	}
}

func (s *Stream) Next() (Event, error) {
	s.frame.Reset()
	var e Event
	var data [][]byte
	seen := false
	size := 0
	for {
		line, err := s.line()
		if err != nil {
			if errors.Is(err, io.EOF) && seen {
				err = io.ErrUnexpectedEOF
			}
			if s.frame.Len() > 0 {
				prefix := append([]byte(nil), s.frame.Bytes()...)
				return Event{}, &RejectedFrame{ID: e.ID, Type: e.Type, Cause: err, Reader: io.MultiReader(bytes.NewReader(prefix), &frameTail{reader: s.r, partial: s.partialLine})}
			}
			return Event{}, err
		}
		if len(line) == 0 {
			if !seen {
				s.frame.Reset()
				continue
			}
			e.Data = bytes.Join(data, []byte{'\n'})
			if e.Type == "" {
				e.Type = "message"
			}
			return e, nil
		}
		if line[0] == ':' {
			continue
		}
		size += len(line) + 1
		if size > s.maxLine {
			return Event{}, &RejectedFrame{ID: e.ID, Type: e.Type, Cause: ErrEventTooLong, Reader: io.MultiReader(bytes.NewReader(append([]byte(nil), s.frame.Bytes()...)), &frameTail{reader: s.r})}
		}
		seen = true
		field, value, _ := bytes.Cut(line, []byte{':'})
		value = bytes.TrimPrefix(value, []byte{' '})
		switch string(field) {
		case "event":
			e.Type = string(value)
		case "data":
			data = append(data, append([]byte(nil), value...))
		case "id":
			if !bytes.ContainsRune(value, 0) {
				e.ID = string(value)
			}
		}
	}
}

// RejectedFrame exposes the exact frame, including delimiters, as a bounded
// stream. The acquisition owner can quarantine an oversized event without
// buffering its tail or consuming the following event.
type RejectedFrame struct {
	ID, Type string
	Cause    error
	Reader   io.Reader
}

func (e *RejectedFrame) Error() string { return e.Cause.Error() }
func (e *RejectedFrame) Unwrap() error { return e.Cause }

type frameTail struct {
	reader        *bufio.Reader
	pending       []byte
	partial, done bool
}

func (r *frameTail) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.pending) > 0 {
		n := copy(p, r.pending)
		r.pending = r.pending[n:]
		return n, nil
	}
	if r.done {
		return 0, io.EOF
	}
	line, e := r.reader.ReadSlice('\n')
	blank := !r.partial && (bytes.Equal(line, []byte{'\n'}) || bytes.Equal(line, []byte{'\r', '\n'}))
	r.partial = e == bufio.ErrBufferFull
	if blank || e == io.EOF {
		r.done = true
	}
	if e != nil && e != bufio.ErrBufferFull && e != io.EOF {
		return 0, e
	}
	r.pending = append(r.pending, line...)
	if len(r.pending) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}
