package rsm

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dRSM     logTopic = "RSM "
	dApply   logTopic = "APPLY"
	dSubmit  logTopic = "SUBM"
	dPending logTopic = "PEND"
	dState   logTopic = "STATE"
	dOp      logTopic = "OP   "
	dOrder   logTopic = "ORDER"
)

// Debugging
const isDebug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if isDebug {
		log.Printf(format, a...)
	}
	return
}

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
