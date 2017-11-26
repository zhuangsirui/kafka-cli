package main

import (
	"io"
	"os"
)

type writer struct {
	file   *os.File
	writer io.Writer
}

func newWriter(output string) (*writer, error) {
	r := new(writer)
	r.writer = os.Stdout
	if output != "" {
		if file, err := os.Create(output); err != nil {
			return nil, err
		} else {
			r.file = file
			r.writer = file
		}
	}
	return r, nil
}

func (r *writer) write(in []byte) error {
	_, err := r.writer.Write(in)
	return err
}

func (r *writer) close() {
	if r.file != nil {
		r.file.Close()
	}
}
