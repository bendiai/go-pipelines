package pipelines

import (
	"log"
	"os"
)

const (
	ErrorLogName     = "failed.log"
	ExecutionLogName = "execution.log"
)

type LogWriter = func(message string) (*os.File, error)

func CreateAppendErrorLog(location string) (*os.File, error) {
	errorLog, err := os.OpenFile(location+"-"+ErrorLogName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	return errorLog, err
}

func CreateAppendExecutionLog(location string) (*os.File, error) {
	executionLog, err := os.OpenFile(location+"-"+ExecutionLogName,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	return executionLog, err
}

func DeleteIfEmptyOrClose(f *os.File) {
	info, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}

	if info.Size() == 0 {
		os.Remove(f.Name())
	} else {
		f.Close()
	}
}
