package satellite

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

type Event struct {
	ID   string
	Type string
	Data []byte
}

type Stream struct {
	r       *bufio.Reader
	maxLine int
}

var ErrLineTooLong = errors.New("server-sent event line exceeds the accepted size")
var ErrEventTooLong = errors.New("server-sent event exceeds the accepted size")

func NewStream(r io.Reader, maxLine int) *Stream {
	return &Stream{r: bufio.NewReaderSize(r, 64<<10), maxLine: maxLine}
}

func (s *Stream) line() ([]byte, error) {
	var out []byte
	for {
		chunk, prefix, err := s.r.ReadLine()
		if err != nil {
			return nil, err
		}
		out = append(out, chunk...)
		if len(out) > s.maxLine {
			return nil, ErrLineTooLong
		}
		if !prefix {
			return out, nil
		}
	}
}

func (s *Stream) Next() (Event, error) {
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
			return Event{}, err
		}
		if len(line) == 0 {
			if !seen {
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
			return Event{}, ErrEventTooLong
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
