package main

import (
	"bufio"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/uluyol/oneoffs/u1proc"
)

type ID struct {
	User, Vol, Node int64
}

type writer struct {
	w *bufio.Writer
	c chan wmesg
	e atomic.Value
}

type wmesg struct {
	fid   int64
	age   time.Duration
	isPut bool
}

func (w *writer) Emit(fid int64, isPut bool, age time.Duration) bool {
	e := w.e.Load()
	if e != nil {
		return false
	}
	w.c <- wmesg{fid: fid, age: age, isPut: isPut}
	return true
}

func (w *writer) close() { close(w.c) }

func (w *writer) start() {
	w.c = make(chan wmesg, 4*runtime.NumCPU())

	go func() {
		wroteErr := false
		for m := range w.c {
			var err error
			w.emitOne(&err, m.fid, m.isPut, m.age)
			if err != nil && !wroteErr {
				w.e.Store(err)
			}
		}
	}()
}

func (w *writer) emitOne(err *error, fid int64, isPut bool, age time.Duration) {
	if *err != nil {
		return
	}
	_, *err = w.w.WriteString(strconv.FormatInt(fid, 10))
	if *err != nil {
		return
	}
	if isPut {
		_, *err = w.w.WriteString(",Write,")
		if *err != nil {
			return
		}
	} else {
		_, *err = w.w.WriteString(",Read,")
		if *err != nil {
			return
		}
	}
	ageMillis := int64(age / time.Millisecond)
	_, *err = w.w.WriteString(strconv.FormatInt(ageMillis, 10))
	if *err != nil {
		return
	}
	_, *err = w.w.WriteString("\n")
	if *err != nil {
		return
	}
	return
}

func (w *writer) Err() error {
	e := w.e.Load()
	if e == nil {
		return nil
	}
	return e.(error)
}

func main() {
	log.SetPrefix("u1-popularity-ts: ")
	log.SetFlags(0)

	rdr := u1proc.NewReader(os.Stdin)
	nextFID := int64(1)
	fids := make(map[ID]int64)
	var starts []time.Time

	bw := bufio.NewWriterSize(os.Stdout, 40960)
	defer bw.Flush()
	w := writer{w: bw}
	w.start()
	defer w.close()

	for rdr.Scan() {
		r := rdr.Record()
		isPut := false
		if r.ReqType != "PutContentResponse" && r.ReqType != "GetContentResponse" {
			continue
		}
		isPut = r.ReqType == "PutContentResponse"
		if r.Type != "storage_done" {
			continue
		}

		id := ID{r.UserID, r.VolID, r.NodeID}
		if id.User < 0 || id.Vol < 0 || id.Node < 0 {
			log.Fatalf("invalid id: %+v", id)
		}

		fid, ok := fids[id]
		if !ok {
			fid = nextFID
			nextFID++
			fids[id] = fid
			var t time.Time
			if isPut {
				t = r.Timestamp.Time()
			}
			starts = append(starts, t)
			if int64(len(starts)) != fid {
				log.Fatal("invariant err: len(starts) != fid")
			}
		}

		if starts[fid-1].IsZero() {
			continue
		}

		age := r.Timestamp.Time().Sub(starts[fid-1])
		if !w.Emit(fid, isPut, age) {
			break
		}
	}

	if rdr.Err() != nil {
		log.Fatal(rdr.Err())
	}
}
