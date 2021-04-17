package main

/**
 * outputdata is a very simple logfile outputter to keep the crawler fast.
 *
 */

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"os"
	"sync"
	"time"
)

// Outputdata
type Outputdata struct {
	n                    string
	period               int64
	current              string
	f                    *os.File
	mu                   sync.Mutex
	Metric_lines_written int

	flush_after_write bool          // Should we call flush after each write?
	gzip              bool          // Gzip the data
	gf                *gzip.Writer  // gzip writer
	fw                *bufio.Writer // buffered writer
}

// GetFilename creates a filename given a starting string and a time period
func GetFilename(n string, period int64, ext string) string {
	// First lets create a time mod period
	tstamp := (time.Now().UTC().Unix() / period) * period
	// Now format it a bit nicer
	tperiod := time.Unix(tstamp, 0).UTC()
	// Now create a nice string for it...
	tstring := fmt.Sprintf("%04d_%02d_%02d__%02d_%02d_%02d", tperiod.Year(), int(tperiod.Month()), tperiod.Day(),
		tperiod.Hour(), tperiod.Minute(), tperiod.Second())

	return fmt.Sprintf("%s_%s.%s", n, tstring, ext)
}

// NewOutputdata creates a new outputdata
func NewOutputdata(n string, period int64) Outputdata {
	return Outputdata{
		n:                 n,
		period:            period,
		flush_after_write: false,
		gzip:              true,
	}
}

// Write some data...
func (od *Outputdata) WriteData(d string) (int, error) {
	od.mu.Lock()
	defer od.mu.Unlock()

	var err error

	// Check if we have the right name...
	ext := ".log"
	if od.gzip {
		ext = ".log.gz"
	}
	ff := GetFilename(od.n, od.period, ext)
	if ff != od.current {
		// Close the current one if any
		if od.f != nil {
			err = od.fw.Flush()
			if err != nil {
				fmt.Printf("Error closing %s %v\n", od.current, err)
			}

			if od.gzip {
				err = od.gf.Close()
				if err != nil {
					fmt.Printf("Error closing %s %v\n", od.current, err)
				}
			}

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

		if od.gzip {
			od.gf = gzip.NewWriter(od.f)
			od.fw = bufio.NewWriter(od.gf)
		} else {
			od.fw = bufio.NewWriter(od.f)
		}
	}

	// Add current time onto the log...
	d = fmt.Sprintf("%d,%s\n", time.Now().Unix(), d)

	l, err := od.fw.WriteString(d)
	if err == nil {
		if od.flush_after_write {
			err = od.fw.Flush()
			if err != nil {
				fmt.Printf("Error flushing!\n")
			}
		}
		od.Metric_lines_written++
	} else {
		fmt.Printf("Error writing to logfile %v\n", err)
	}
	return l, err
}

func (od *Outputdata) Close() {
	od.f.Close()
}
