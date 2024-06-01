package pipelines

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/schollz/progressbar/v3"
)

type sampleStep[T any] struct {
	Step[T]
}

func (s *sampleStep[T]) Run(data Document[T], t *time.Time, pb *progressbar.ProgressBar) Document[T] {
	pb.Add(1)
	s.WriteExecutionLog("Running step")
	s.WriteExecutionLog("Running step")
	pb.Finish()
	return data
}

func TestPipelineIntegration(t *testing.T) {
	type RowExample struct {
		ID    string
		Total int
	}

	pipelineName := "test-pipeline"
	document := Document[RowExample]{Lines: []RowExample{}}

	pipeline := NewPipeline(document, pipelineName)

	pipeline.AddStep(&sampleStep[RowExample]{
		Step: Step[RowExample]{
			WithMeta: WithMeta{
				name: "test-step",
			},
		},
	})

	if !strings.Contains(pipeline.OutputDirName, pipelineName) {
		t.Error()
	}

	output, _ := os.Stat(pipeline.OutputDirName)

	if !output.IsDir() {
		t.Error("Loc is not a directory")
	}

	pipeline.Execute()
	removed := os.RemoveAll(pipeline.OutputDirName)
	if removed == nil {
		t.Log("Could not delete directory " + pipeline.OutputDirName)
	}
}
