package progress

import (
	"io"
	"os"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

var spinner = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// Progress is a multi-progress bar group.
type Progress struct {
	mu        sync.RWMutex
	opts      []mpb.ContainerOption
	container *mpb.Progress
	bars      map[int64]*mpb.Bar
}

// newMPB creates a new mpb progress container.
func newMPB(opts []mpb.ContainerOption) *mpb.Progress {
	return mpb.New(opts...)
}

// WithOutput sets the output for the progress container.
func WithOutput(w io.Writer) func() mpb.ContainerOption {
	return func() mpb.ContainerOption {
		return mpb.WithOutput(w)
	}
}

// WithRefreshRate sets the refresh rate for the progress container.
func WithRefreshRate(refreshRate time.Duration) func() mpb.ContainerOption {
	return func() mpb.ContainerOption {
		return mpb.WithRefreshRate(refreshRate)
	}
}

// WithPopCompletedMode sets the pop completed mode for the progress container.
func WithPopCompletedMode() func() mpb.ContainerOption {
	return func() mpb.ContainerOption {
		return mpb.PopCompletedMode()
	}
}

// NewProgress creates a new progress container.
func NewProgress(opts ...func() mpb.ContainerOption) *Progress {
	containerOpts := DefaultContainerOptions()
	for _, opt := range opts {
		containerOpts = append(containerOpts, opt())
	}
	mpbar := newMPB(containerOpts)
	return &Progress{
		opts:      containerOpts,
		container: mpbar,
		bars:      make(map[int64]*mpb.Bar),
	}
}

// DefaultContainerOptions returns the default container options for the progress container.
func DefaultContainerOptions() []mpb.ContainerOption {
	return []mpb.ContainerOption{
		mpb.WithOutput(os.Stdout),
		mpb.WithRefreshRate(150 * time.Millisecond),
	}
}

// DefaultBarOptions returns the default bar options for the progress container.
func DefaultBarOptions(description string) []mpb.BarOption {
	return []mpb.BarOption{
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Spinner(spinner, decor.WCSyncSpaceR),
			decor.Name(description, decor.WCSyncSpaceR),
			decor.CountersKibiByte("%.2f/%.2f", decor.WCSyncSpace),
			decor.Percentage(decor.WCSyncSpace),
		),
		mpb.AppendDecorators(
			decor.EwmaSpeed(decor.SizeB1024(0), "%.2f", 30, decor.WCSyncSpace),
			decor.EwmaETA(decor.ET_STYLE_GO, 30, decor.WCSyncSpace),
		),
	}
}

// AddBar adds a bar for the given id to the progress container.
func (g *Progress) AddBar(id int64, description string, size int64) {
	g.mu.Lock()
	defer g.mu.Unlock()

	barOpts := DefaultBarOptions(description)
	g.bars[id] = g.container.AddBar(size, barOpts...)
}

// IncrementBar increments the bar for the given id in the progress container.
func (g *Progress) IncrementBar(id int64, n int64, duration time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if bar, ok := g.bars[id]; ok {
		bar.EwmaIncrInt64(n, duration)
	}
}

// UpdateBar updates the bar for the given id in the progress container.
func (g *Progress) UpdateBar(id, n, size int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if bar, ok := g.bars[id]; ok {
		bar.SetTotal(size, false)
		bar.SetCurrent(n)
	}
}

// CloseBar closes the bar for the given id in the progress container.
func (g *Progress) CloseBar(id int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if bar, ok := g.bars[id]; ok {
		bar.Abort(true)
		delete(g.bars, id)
	}
}

// CloseGroup closes the progress container.
func (g *Progress) CloseGroup() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, bar := range g.bars {
		bar.Abort(true)
		bar.Wait()
	}
	g.container.Wait()
	g.bars = make(map[int64]*mpb.Bar)
	g.container = newMPB(g.opts)
}
