package u1proc

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"
)

type FloatOrStr float64

func (v *FloatOrStr) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		s = string(b)
	}

	f, err := strconv.ParseFloat(s, 64)
	*v = FloatOrStr(f)
	return err
}

const timeFormat = "2006-01-02 15:04:05.000"

type Time time.Time

func (v Time) Time() time.Time { return time.Time(v) }

func (v *Time) MarshalJSON() ([]byte, error) {
	return []byte(v.Time().Format(timeFormat)), nil
}

func (v *Time) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("bad time string: %v", err)
	}

	t, err := time.ParseInLocation(timeFormat, s, time.UTC)
	*v = Time(t)
	return err
}

//go:generate ffjson load.go

type Record struct {
	Type           string            `json:"T"`
	Addr           string            `json:"addr"`
	Caps           []string          `json:"caps"`
	ClientMetadata map[string]string `json:"client_metadata"`
	CurrentGen     int64             `json:"current_gen"`
	Ext            string            `json:"ext"`
	Failed         string            `json:"failed"`
	FreeBytes      int64             `json:"free_bytes"`
	FromGen        int64             `json:"from_gen"`
	Hash           int64             `json:"hash"`
	Level          string            `json:"level"`
	LogfileID      string            `json:"logfile_id"`
	Method         string            `json:"method"`
	Mime           string            `json:"mime"`
	Msg            string            `json:"msg"`
	NodeID         int64             `json:"node_id"`
	Nodes          int64             `json:"nodes"`
	PID            int32             `json:"pid"`
	ReqID          int64             `json:"req_id"`
	ReqType        string            `json:"req_t"`
	Root           int64             `json:"root"`
	Server         string            `json:"server"`
	SharedBy       int64             `json:"shared_by"`
	SharedTo       int64             `json:"shared_to"`
	Shares         int64             `json:"shares"`
	SID            string            `json:"sid"` // id of the ubuntuone-storageprotocol session (not http)
	Size           int64             `json:"size"`
	DBTime         FloatOrStr        `json:"time"`
	Timestamp      Time              `json:"tstamp"`
	NodeType       string            `json:"type"`
	UDFS           int64             `json:"udfs"`
	UserID         int64             `json:"user_id"`
	User           int64             `json:"user"`
	VolID          int64             `json:"vol_id"`
}

func (r *Record) reset() {
	r.Caps = nil
	r.ClientMetadata = nil
}

// ffjson: skip
type Reader struct {
	s *bufio.Scanner
	v Record
	e error
}

func NewReader(r io.Reader) *Reader {
	return &Reader{s: bufio.NewScanner(r)}
}

func (r *Reader) Scan() bool {
	var err error

Start:
	if !r.s.Scan() {
		return false
	}
	r.v.reset()
	if err = r.v.UnmarshalJSON(r.s.Bytes()); err != nil {
		if r.e == nil {
			r.e = err
		}
		goto Start
	}

	return true
}

func (r *Reader) Record() Record { return r.v }
func (r *Reader) Err() error     { return r.e }
