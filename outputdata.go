package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Outputdata struct {
	f                    *os.File
	mu                   sync.Mutex
	Metric_lines_written int
}

func NewOutputdata(n string) (Outputdata, error) {
	f, err := os.Create(n)
	if err != nil {
		return Outputdata{}, err
	}
	return Outputdata{
		f: f,
	}, nil
}

func (od *Outputdata) WriteData(d string) (int, error) {
	od.mu.Lock()
	defer od.mu.Unlock()

	// Add current time onto the log...
	d = fmt.Sprintf("%d,%s\n", time.Now().Unix(), d)

	l, err := od.f.WriteString(d)
	if err == nil {
		od.Metric_lines_written++
	}
	return l, err
}

func (od *Outputdata) Close() {
	od.f.Close()
}
