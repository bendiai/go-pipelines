package pipelines

import (
	"fmt"
	"log"
	"os"
	"time"

	progressbar "github.com/schollz/progressbar/v3"
)

const (
	fileModeRealAll = 0700
	timeStampFormat = "%d-%02d-%02d-%d"
	logFormat       = "%s %s\n"
)

type Document[L any] struct {
	Lines []L
}

// Used for embedding
type WithMeta struct {
	name    string
	Started *time.Time
	Ended   *time.Time
}

type pipeline[RowType any] struct {
	steps         []IStep[RowType]
	OutputDirName string
	Input         Document[RowType]
	WithMeta
}

type IStep[T any] interface {
	Run(data Document[T], startTime *time.Time, bar *progressbar.ProgressBar) Document[T]
	GetName() string
	SetPipeline(pipeline *pipeline[T])
	WriteExecutionLog(message string) (*os.File, error)
	WriteErrorLog(message string) (*os.File, error)
}

type Step[T any] struct {
	WithMeta
	Pipeline *pipeline[T]
}

func getRootLocation[T any](s *Step[T]) string {
	return s.Pipeline.OutputDirName + "/" + s.name
}

func formatLog(m string) string {
	return fmt.Sprintf(logFormat, time.Now().Format(time.RFC3339Nano), m)
}

func (s *Step[RowType]) WriteExecutionLog(message string) (*os.File, error) {
	executionLog, err := CreateAppendExecutionLog(getRootLocation(s))
	defer DeleteIfEmptyOrClose(executionLog)
	executionLog.WriteString(formatLog(message))
	return executionLog, err
}

func (s *Step[RowType]) WriteErrorLog(message string) (*os.File, error) {
	errorLog, err := CreateAppendErrorLog(getRootLocation(s))
	defer DeleteIfEmptyOrClose(errorLog)
	errorLog.WriteString(formatLog(message))
	return errorLog, err
}

func NewPipeline[RowType any](input Document[RowType], name string) *pipeline[RowType] {
	now := time.Now()

	timestamp := fmt.Sprintf(timeStampFormat,
		now.Year(), now.Month(), now.Day(),
		now.UnixMicro())

	outputDirName := "outputs/" + name + "-" + timestamp

	if _, err := os.Stat(outputDirName); os.IsNotExist(err) {
		if err := os.MkdirAll(outputDirName, fileModeRealAll); err != nil {
			log.Fatal(fmt.Errorf("Could not create directory "+outputDirName+" : %v", err))
		}
	}
	return &pipeline[RowType]{
		OutputDirName: outputDirName,
		WithMeta:      WithMeta{name: name, Started: &now},
		Input:         input,
	}
}

func (p *pipeline[RowType]) AddStep(s IStep[RowType]) IStep[RowType] {
	p.steps = append(p.steps, s)
	return s
}

func (p *pipeline[RowType]) Execute() Document[RowType] {
	log.Print("Starting pipeline: " + p.name)

	pipelineStart := time.Now()
	p.Started = &pipelineStart

	for _, step := range p.steps {
		step.SetPipeline(p)
		stepStart := time.Now()
		log.Printf("Running step: '%s'", step.GetName())
		p.Input = step.Run(
			p.Input,
			&stepStart,
			progressbar.Default(int64(len(p.Input.Lines))),
		)
	}
	pipelineEnd := time.Now()
	p.Ended = &pipelineEnd

	log.Printf("Pipeline completed in %s.",
		time.Since(pipelineStart),
	)
	return p.Input
}

func (s *Step[T]) SetPipeline(p *pipeline[T]) {
	s.Pipeline = p
}

func (s *Step[T]) GetName() string {
	return s.name
}
