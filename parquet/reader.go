package parquet

import (
	"context"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/pkg/errors"
)

// Reader handles reading Arrow RecordBatches from Parquet files.
type Reader struct {
	path   string
	file   *os.File
	reader *pqarrow.FileReader
}

// NewReader creates a new Parquet reader for the given path.
func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open parquet file")
	}

	pf, err := file.NewParquetReader(f)
	if err != nil {
		f.Close()
		return nil, errors.Wrap(err, "failed to create parquet reader")
	}

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: 65536,
	}, nil)
	if err != nil {
		pf.Close()
		f.Close()
		return nil, errors.Wrap(err, "failed to create arrow file reader")
	}

	return &Reader{
		path:   path,
		file:   f,
		reader: reader,
	}, nil
}

// ReadTable reads the entire parquet file as an Arrow Table.
func (r *Reader) ReadTable(ctx context.Context) (arrow.Table, error) {
	table, err := r.reader.ReadTable(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read parquet table")
	}
	return table, nil
}

// NumRowGroups returns the number of row groups in the file.
func (r *Reader) NumRowGroups() int {
	return r.reader.ParquetReader().NumRowGroups()
}

// ReadRowGroup reads a specific row group as a RecordBatch.
func (r *Reader) ReadRowGroup(ctx context.Context, rowGroup int) (arrow.Record, error) {
	rr, err := r.reader.GetRecordReader(ctx, nil, []int{rowGroup})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get record reader")
	}
	defer rr.Release()

	if !rr.Next() {
		return nil, nil
	}

	record := rr.Record()
	record.Retain()
	return record, nil
}

// Close closes the reader and underlying file.
func (r *Reader) Close() error {
	if r.reader != nil {
		r.reader.ParquetReader().Close()
	}
	if r.file != nil {
		if err := r.file.Close(); err != nil {
			return errors.Wrap(err, "failed to close file")
		}
	}
	return nil
}

// DataReaders holds readers for all data types.
type DataReaders struct {
	Blocks       *Reader
	Transactions *Reader
	Logs         *Reader
	Traces       *Reader
}

// NewDataReaders creates readers for all data types in the given directory.
// Returns nil for any file that doesn't exist.
func NewDataReaders(dir string) (*DataReaders, error) {
	readers := &DataReaders{}

	blocksPath := filepath.Join(dir, "blocks.parquet")
	if _, err := os.Stat(blocksPath); err == nil {
		r, err := NewReader(blocksPath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to open blocks.parquet")
		}
		readers.Blocks = r
	}

	txPath := filepath.Join(dir, "transactions.parquet")
	if _, err := os.Stat(txPath); err == nil {
		r, err := NewReader(txPath)
		if err != nil {
			readers.Close()
			return nil, errors.Wrap(err, "failed to open transactions.parquet")
		}
		readers.Transactions = r
	}

	logsPath := filepath.Join(dir, "logs.parquet")
	if _, err := os.Stat(logsPath); err == nil {
		r, err := NewReader(logsPath)
		if err != nil {
			readers.Close()
			return nil, errors.Wrap(err, "failed to open logs.parquet")
		}
		readers.Logs = r
	}

	tracesPath := filepath.Join(dir, "traces.parquet")
	if _, err := os.Stat(tracesPath); err == nil {
		r, err := NewReader(tracesPath)
		if err != nil {
			readers.Close()
			return nil, errors.Wrap(err, "failed to open traces.parquet")
		}
		readers.Traces = r
	}

	return readers, nil
}

// Close closes all readers.
func (dr *DataReaders) Close() error {
	var errs []error

	if dr.Blocks != nil {
		if err := dr.Blocks.Close(); err != nil {
			errs = append(errs, errors.Wrap(err, "blocks"))
		}
	}
	if dr.Transactions != nil {
		if err := dr.Transactions.Close(); err != nil {
			errs = append(errs, errors.Wrap(err, "transactions"))
		}
	}
	if dr.Logs != nil {
		if err := dr.Logs.Close(); err != nil {
			errs = append(errs, errors.Wrap(err, "logs"))
		}
	}
	if dr.Traces != nil {
		if err := dr.Traces.Close(); err != nil {
			errs = append(errs, errors.Wrap(err, "traces"))
		}
	}

	if len(errs) > 0 {
		return errors.Errorf("failed to close readers: %v", errs)
	}
	return nil
}
