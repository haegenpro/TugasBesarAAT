package wildfire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func text(s string) bool    { return s != "" && utf8.ValidString(s) && !strings.ContainsRune(s, 0) }
func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
func validCoordinates(lat, lon float64) bool {
	return finite(lat) && finite(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180
}
func parse(d Detection, offset int64, payload []byte) (Observation, error) {
	tuple := [5]string{d.Satellite, d.AcqDate, d.AcqTime, d.Latitude, d.Longitude}
	for _, s := range tuple {
		if !text(s) {
			return Observation{}, fmt.Errorf("invalid detection key component")
		}
	}
	date, err := time.Parse("2006-01-02", d.AcqDate)
	if err != nil {
		return Observation{}, fmt.Errorf("invalid UTC acquisition date")
	}
	if len(d.AcqTime) < 1 || len(d.AcqTime) > 4 {
		return Observation{}, fmt.Errorf("invalid HHMM")
	}
	for _, r := range d.AcqTime {
		if r < '0' || r > '9' {
			return Observation{}, fmt.Errorf("invalid HHMM")
		}
	}
	hhmm, _ := strconv.Atoi(d.AcqTime)
	if hhmm/100 > 23 || hhmm%100 > 59 {
		return Observation{}, fmt.Errorf("invalid HHMM")
	}
	at := date.Add(time.Duration(hhmm/100)*time.Hour + time.Duration(hhmm%100)*time.Minute)
	lat, e1 := strconv.ParseFloat(d.Latitude, 64)
	lon, e2 := strconv.ParseFloat(d.Longitude, 64)
	if e1 != nil || e2 != nil || !validCoordinates(lat, lon) {
		return Observation{}, fmt.Errorf("invalid coordinates")
	}
	if d.Confidence != "l" && d.Confidence != "n" && d.Confidence != "h" {
		return Observation{}, fmt.Errorf("fixture confidence must be l, n or h")
	}
	if !finite(d.FRPMW) || d.FRPMW < 0 {
		return Observation{}, fmt.Errorf("invalid FRP")
	}
	return Observation{Key: strings.Join(tuple[:], "|"), Tuple: tuple, Detection: &d, Latitude: lat, Longitude: lon, EventTime: &at, Active: true, LastOffset: offset, Payload: append([]byte(nil), payload...)}, nil
}
func validJSON(raw []byte) error {
	if !utf8.Valid(raw) {
		return fmt.Errorf("invalid UTF-8")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var value func(int) error
	value = func(depth int) error {
		if depth > 64 {
			return fmt.Errorf("JSON nesting exceeds 64")
		}
		token, e := d.Token()
		if e != nil {
			return e
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return fmt.Errorf("duplicate JSON key")
				}
				seen[s] = true
				if e = value(depth + 1); e != nil {
					return e
				}
			}
		case '[':
			for d.More() {
				if e := value(depth + 1); e != nil {
					return e
				}
			}
		default:
			return fmt.Errorf("invalid JSON delimiter")
		}
		_, e = d.Token()
		return e
	}
	if err := value(0); err != nil {
		return err
	}
	if _, e := d.Token(); e != io.EOF {
		return fmt.Errorf("trailing JSON")
	}
	return nil
}
