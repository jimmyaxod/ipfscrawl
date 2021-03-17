package main

/**
 * outputdata is a very simple logfile outputter to keep the crawler fast.
 *
 * TODO: Batched output
 *
 */

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Outputdata struct {
	n                    string
	period               int64
	current              string
	f                    *os.File
	mu                   sync.Mutex
	Metric_lines_written int
}

// GetFilename creates a filename given a starting string and a time period
func GetFilename(n string, period int64) string {
	// First lets create a time mod period
	tstamp := (time.Now().UTC().Unix() / period) * period
	// Now format it a bit nicer
	tperiod := time.Unix(tstamp, 0).UTC()
	// Now create a nice string for it...
	tstring := fmt.Sprintf("%04d_%02d_%02d__%02d_%02d_%02d", tperiod.Year(), int(tperiod.Month()), tperiod.Day(),
		tperiod.Hour(), tperiod.Minute(), tperiod.Second())
	return fmt.Sprintf("%s_%s.log", n, tstring)
}

// NewOutputdata creates a new outputdata
func NewOutputdata(n string, period int64) Outputdata {
	return Outputdata{
		n:      n,
		period: period,
	}
}

// Write some data...
func (od *Outputdata) WriteData(d string) (int, error) {
	od.mu.Lock()
	defer od.mu.Unlock()

	var err error

	// Check if we have the right name...
	ff := GetFilename(od.n, od.period)
	if ff != od.current {
		// Close the current one if any
		if od.f != nil {
			err = od.f.Close()
			if err != nil {
				fmt.Printf("Error closing %s %v\n", od.current, err)
			}
		}

		// Now open the new one...
		od.current = ff
		od.f, err = os.Create(ff)
		if err != nil {
			fmt.Printf("Error creating new logfile %s %v\n", ff, err)
		}
	}

	// Add current time onto the log...
	d = fmt.Sprintf("%d,%s\n", time.Now().Unix(), d)

	l, err := od.f.WriteString(d)
	if err == nil {
		od.Metric_lines_written++
	} else {
		fmt.Printf("Error writing to logfile %v\n", err)
	}
	return l, err
}

func (od *Outputdata) Close() {
	od.f.Close()
}
