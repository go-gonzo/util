package util

import (
	"sync"

	"github.com/omeid/gonzo"
	"github.com/omeid/gonzo/context"
)

// Merge concurrently Merges the output of multiple chan of gonzo.File into a pipe.
func Merge(ctx context.Context, pipes ...gonzo.Pipe) gonzo.Pipe {

	ctx, cancel := context.WithCancel(ctx)
	for _, pipe := range pipes {
		go func(c context.Context) {
			<-c.Done()
			cancel()
		}(pipe.Context())
	}

	out := make(chan gonzo.File)

	go func(out chan gonzo.File) {
		var wg sync.WaitGroup
		wg.Add(len(pipes))
		defer close(out)

		for _, p := range pipes {
			go func(p gonzo.Pipe) {
				defer wg.Done()
				files := p.Files()
				ctx := p.Context()
				for {
					select {
					case f, ok := <-files:
						if !ok {
							return
						}
						out <- f
					case <-ctx.Done():
						return
					}
				}
			}(p)
		}
		wg.Wait()
	}(out)

	return gonzo.NewPipe(ctx, out)
}

// Merges the output of multiple chan of gonzo.File into a pipe in a serial manner.
// (i.e Reads first chan until the end and moves to the next until the last channel is finished.
func Queue(pipe gonzo.Pipe, pipes ...gonzo.Pipe) gonzo.Pipe {

	if len(pipes) == 0 {
		return pipe
	}

	pipes = append([]gonzo.Pipe{pipe}, pipes...)

	ctx, cancel := context.WithCancel(pipe.Context())
	for _, pipe := range pipes {
		go func(c context.Context) {
			<-c.Done()
			cancel()
		}(pipe.Context())
	}

	out := make(chan gonzo.File)

	go func(out chan gonzo.File) {
		defer close(out)

		for _, p := range pipes {
			func(files <-chan gonzo.File) {
				for {
					select {
					case f, ok := <-files:
						if !ok {
							return
						}
						out <- f
					case <-ctx.Done():
						return
					}
				}
			}(p.Files())

			if ctx.Err() != nil {
				return
			}
		}
	}(out)

	return gonzo.NewPipe(ctx, out)
}
