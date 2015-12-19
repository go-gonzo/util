package util

import (
	"bytes"
	"io/ioutil"
	"path/filepath"

	"github.com/omeid/gonzo"
	"github.com/omeid/gonzo/context"
)

// Concatenates all the files from the input channel
// and passes them to output channel with the given name.
func Concat(ctx context.Context, name string) gonzo.Stage {
	return func(ctx context.Context, files <-chan gonzo.File, out chan<- gonzo.File) error {

		var (
			size    int64
			bigfile = new(bytes.Buffer)
		)

		err := func() error {
			for {
				select {
				case f, ok := <-files:
					if !ok {
						return nil
					}

					ctx.Infof(
						"Adding %s to %s",
						filepath.Join(f.FileInfo().Base(), f.FileInfo().Name()),
						name,
					)

					n, err := bigfile.ReadFrom(f)
					if err != nil {
						return err
					}
					bigfile.WriteRune('\n')
					size += n + 1

					f.Close()
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}()

		if err != nil {
			return err
		}

		file := gonzo.NewFile(ioutil.NopCloser(bigfile), gonzo.NewFileInfo())
		file.FileInfo().SetSize(size)
		file.FileInfo().SetName(name)
		out <- file
		return nil
	}
}

// A simple transformation gonzoStage, sends the file to output
// channel after passing it through the the "do" function.
func Do(do func(gonzo.File) gonzo.File) gonzo.Stage {
	return func(ctx context.Context, in <-chan gonzo.File, out chan<- gonzo.File) error {
		for {
			select {
			case file, ok := <-in:
				if !ok {
					return nil
				}
				out <- do(file)

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

//For The Glory of Debugging.
func List(checkpoint string) gonzo.Stage {
	return func(ctx context.Context, in <-chan gonzo.File, out chan<- gonzo.File) error {
		ctx = context.WithValue(ctx, "checkpoint", checkpoint)

		for {
			select {
			case file, ok := <-in:
				if !ok {
					return nil
				}
				s, err := file.Stat()
				if err != nil {
					ctx.Error("Can't get File Stat name.")
				} else {
					ctx.Infof("File %s", s.Name())
				}
				out <- file

			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
