package mr

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	dClient      logTopic = "CLNT"
	dCoordinator logTopic = "CORD"
	dWorker      logTopic = "WORK"
	dMap         logTopic = "MAP "
	dReduce      logTopic = "REDU"
	dTask        logTopic = "TASK"
	dFile        logTopic = "FILE"
	dRPC         logTopic = "RPC "
	dError       logTopic = "ERRO"
	dInfo        logTopic = "INFO"
	dWarn        logTopic = "WARN"
	dTest        logTopic = "TEST"
	dTrace       logTopic = "TRCE"
	dHeartbeat   logTopic = "BEAT"
	dTimeout     logTopic = "TIME"
)

// 调试开关，可以通过修改这个值来开启/关闭调试
const isDebug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if isDebug {
		log.Printf(format, a...)
	}
	return
}

// 从环境变量获取详细程度级别
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

// Debug 主调试函数
func Debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// DebugLevel 带级别的调试函数，级别越高需要更高的verbosity
func DebugLevel(level int, topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= level {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// 向后兼容的Debuger函数
func Debuger() {
	Debug(dTrace, "Legacy debug call")
}
