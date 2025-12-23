package parquet

import (
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/parquet"
	"github.com/apache/arrow/go/v10/parquet/compress"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
	"github.com/pkg/errors"
)

// Writer handles writing Arrow RecordBatches to Parquet files.
type Writer struct {
	path       string
	file       *os.File
	writer     *pqarrow.FileWriter
	schema     *arrow.Schema
	initialized bool
}

// NewWriter creates a new Parquet writer for the given path.
// The schema is set on the first write.
func NewWriter(path string) *Writer {
	return &Writer{
		path: path,
	}
}

// Write writes an Arrow RecordBatch to the Parquet file.
// On first call, it initializes the file and schema.
func (w *Writer) Write(record arrow.Record) error {
	if !w.initialized {
		if err := w.initialize(record.Schema()); err != nil {
			return errors.Wrap(err, "failed to initialize parquet writer")
		}
	}

	if err := w.writer.WriteBuffered(record); err != nil {
		return errors.Wrap(err, "failed to write record batch")
	}

	return nil
}

func (w *Writer) initialize(schema *arrow.Schema) error {
	dir := filepath.Dir(w.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return errors.Wrap(err, "failed to create directory")
	}

	file, err := os.Create(w.path)
	if err != nil {
		return errors.Wrap(err, "failed to create parquet file")
	}
	w.file = file
	w.schema = schema

	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(true),
		parquet.WithStats(true),
	)

	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
	)

	writer, err := pqarrow.NewFileWriter(schema, file, props, arrowProps)
	if err != nil {
		file.Close()
		return errors.Wrap(err, "failed to create parquet writer")
	}
	w.writer = writer
	w.initialized = true

	return nil
}

// Close closes the Parquet writer and underlying file.
func (w *Writer) Close() error {
	if w.writer != nil {
		if err := w.writer.Close(); err != nil {
			return errors.Wrap(err, "failed to close parquet writer")
		}
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return errors.Wrap(err, "failed to close file")
		}
	}
	return nil
}

// HasData returns true if any data has been written.
func (w *Writer) HasData() bool {
	return w.initialized
}

// DataWriters holds writers for all data types.
type DataWriters struct {
	Blocks       *Writer
	Transactions *Writer
	Logs         *Writer
	Traces       *Writer
}

// NewDataWriters creates writers for all data types in the given directory.
func NewDataWriters(dir string) *DataWriters {
	return &DataWriters{
		Blocks:       NewWriter(filepath.Join(dir, "blocks.parquet")),
		Transactions: NewWriter(filepath.Join(dir, "transactions.parquet")),
		Logs:         NewWriter(filepath.Join(dir, "logs.parquet")),
		Traces:       NewWriter(filepath.Join(dir, "traces.parquet")),
	}
}

// Close closes all writers.
func (dw *DataWriters) Close() error {
	var errs []error

	if err := dw.Blocks.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "blocks"))
	}
	if err := dw.Transactions.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "transactions"))
	}
	if err := dw.Logs.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "logs"))
	}
	if err := dw.Traces.Close(); err != nil {
		errs = append(errs, errors.Wrap(err, "traces"))
	}

	if len(errs) > 0 {
		return errors.Errorf("failed to close writers: %v", errs)
	}
	return nil
}
