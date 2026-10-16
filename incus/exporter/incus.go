/*
 * Copyright (c) 2026 Antoine Dheygers <antoine.dheygers@cryptoweb.fr>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package exporter

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/location"
)

func init() {
	_ = exporter.Register("incus", 0, NewExporter)
}

// restoreSink abstracts the Incus restore API for testability.
type restoreSink interface {
	Ping(ctx context.Context) error
	// Restore consumes a portable backup tar stream and creates the
	// named instance from it.
	Restore(ctx context.Context, instance string, tarStream io.Reader) error
}

type Exporter struct {
	instance string
	project  string
	sink     restoreSink
}

func NewExporter(ctx context.Context, opts *connectors.Options, proto string, config map[string]string) (exporter.Exporter, error) {
	instance := strings.TrimPrefix(config["location"], "incus://")
	if instance == "" || strings.Contains(instance, "/") {
		return nil, fmt.Errorf("incus: invalid instance name %q", instance)
	}
	sink, err := newIncusSink(config)
	if err != nil {
		return nil, err
	}
	exp := newExporterWithSink(instance, sink)
	exp.project = config["project"]
	return exp, nil
}

func newExporterWithSink(instance string, sink restoreSink) *Exporter {
	return &Exporter{instance: instance, sink: sink}
}

func (p *Exporter) Type() string          { return "incus" }
func (p *Exporter) Root() string          { return "/" }
func (p *Exporter) Flags() location.Flags { return 0 }

func (p *Exporter) Origin() string {
	// Same qualification rule as the importer: instance names are only
	// unique within a project, so a non-default project prefixes the
	// origin to keep it unambiguous.
	if p.project != "" {
		return p.project + "/" + p.instance
	}
	return p.instance
}

func (p *Exporter) Ping(ctx context.Context) error { return p.sink.Ping(ctx) }

func (p *Exporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) (ret error) {
	defer close(results)

	pr, pw := io.Pipe()
	sinkDone := make(chan error, 1)
	go func() {
		err := p.sink.Restore(ctx, p.instance, pr)
		// Always close the read end, even when Restore returns early
		// without draining the stream: otherwise the tar writer below
		// stays blocked forever on pw.Write, deadlocking Export when
		// the sink rejects the upload instead of consuming it fully.
		pr.CloseWithError(err)
		sinkDone <- err
	}()

	tw := tar.NewWriter(pw)

	// The tar stream is sequential by nature: records are written in
	// the order they arrive, one at a time.
	//
	// The rebuilt tar preserves record ARRIVAL ORDER, and Incus expects
	// backup/index.yaml first. Correctness here relies on kloset replaying
	// records in import order (true today, beta-validated); if kloset ever
	// reorders records, this needs revisiting.
	//
	// Each record is written (and acked) as soon as it arrives. Over the
	// plugin gRPC transport a record's body is streamed in-band and the
	// next record is only delivered once this one's Reader is closed (by
	// Ok/Error): holding a record back to wait for a later one deadlocks
	// the restore on the first regular file (seen live, v0.1.3/v0.1.4).
	// Consequence: xattr records cannot be folded into an already written
	// header. kloset only sends them after the whole tree, and only to
	// exporters advertising FLAG_EXPORTXATTR (this one does not), so none
	// arrive in practice.
	writeRecord := func(p *connectors.Record) error {
		hdr, err := tarHeader(p)
		if err != nil {
			results <- p.Error(err)
			return nil
		}
		if hdr.Typeflag == tar.TypeReg && hdr.Size > 0 && p.Reader == nil {
			// Writing this header would promise hdr.Size bytes of
			// body that we have nothing to supply, corrupting the
			// tar stream for every entry that follows. Reject the
			// record instead of emitting a header with no body.
			results <- p.Error(fmt.Errorf("incus: record %q has size %d but no reader", p.Pathname, hdr.Size))
			return nil
		}
		if err := tw.WriteHeader(hdr); err != nil {
			results <- p.Error(err)
			return err
		}
		if hdr.Typeflag == tar.TypeReg && p.Reader != nil {
			n, err := io.Copy(tw, p.Reader)
			if err != nil {
				results <- p.Error(err)
				return err
			}
			if n != hdr.Size {
				// The tar stream is already corrupted at this point (the
				// header promised hdr.Size bytes of body): a short read
				// here would otherwise surface as a cryptic error on the
				// *next* WriteHeader call. Fail this record now with a
				// clear message instead.
				err := fmt.Errorf("incus: %s: short read: got %d bytes, want %d", p.Pathname, n, hdr.Size)
				results <- p.Error(err)
				return err
			}
		}
		results <- p.Ok()
		return nil
	}

loop:
	for {
		select {
		case <-ctx.Done():
			ret = ctx.Err()
			break loop

		case rec, ok := <-records:
			if !ok {
				break loop
			}
			if rec.Err != nil {
				results <- rec.Ok()
				continue
			}
			if rec.IsXattr {
				results <- rec.Error(fmt.Errorf("incus: xattr record %q (%s): extended attributes cannot be restored by this exporter", rec.Pathname, rec.XattrName))
				continue
			}
			if err := writeRecord(rec); err != nil {
				ret = err
				break loop
			}
		}
	}

	if err := tw.Close(); err != nil && ret == nil {
		ret = err
	}
	if ret != nil {
		pw.CloseWithError(ret)
	} else {
		_ = pw.Close()
	}

	// The sink's error is the actionable one: it explains *why* the pipe
	// broke. When the write loop only observed the pipe closing (nil, or
	// the unhelpful io.ErrClosedPipe), surface the sink's error instead of
	// masking it.
	if sinkErr := <-sinkDone; sinkErr != nil && (ret == nil || errors.Is(ret, io.ErrClosedPipe)) {
		ret = fmt.Errorf("incus: restore %s: %w", p.instance, sinkErr)
	}
	return ret
}

func (p *Exporter) Close(ctx context.Context) error { return nil }
