// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

package internal

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// ipcBytesToRecord deserializes Arrow IPC bytes into a single arrow.Record.
// If the IPC file contains multiple batches, they are concatenated.
// Returns nil, nil for nil/empty input (empty result set).
// The caller is responsible for calling Release() on the returned Record (if non-nil).
func ipcBytesToRecord(ipcBytes []byte) (arrow.Record, error) {
	if len(ipcBytes) == 0 {
		return nil, nil
	}

	reader, err := ipc.NewFileReader(bytes.NewReader(ipcBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer reader.Close()

	schema := reader.Schema()
	numRecords := reader.NumRecords()

	if numRecords == 0 {
		pool := memory.NewGoAllocator()
		cols := make([]arrow.Array, schema.NumFields())
		for i, field := range schema.Fields() {
			builder := array.NewBuilder(pool, field.Type)
			cols[i] = builder.NewArray()
			builder.Release()
		}
		return array.NewRecord(schema, cols, 0), nil
	}

	if numRecords == 1 {
		rec, err := reader.Record(0)
		if err != nil {
			return nil, fmt.Errorf("failed to read record batch: %w", err)
		}
		rec.Retain()
		return rec, nil
	}

	// Multiple batches — collect and concatenate
	records := make([]arrow.Record, 0, numRecords)
	for i := 0; i < numRecords; i++ {
		rec, err := reader.Record(i)
		if err != nil {
			return nil, fmt.Errorf("failed to read record batch %d: %w", i, err)
		}
		rec.Retain()
		records = append(records, rec)
	}
	defer func() {
		for _, r := range records {
			r.Release()
		}
	}()

	table := array.NewTableFromRecords(schema, records)
	defer table.Release()

	tr := array.NewTableReader(table, table.NumRows())
	defer tr.Release()
	if tr.Next() {
		rec := tr.Record()
		rec.Retain()
		return rec, nil
	}

	return nil, fmt.Errorf("failed to concatenate record batches")
}

// arrowRecordToMaps converts an Arrow Record to []map[string]interface{}.
// Utility for callers who want the convenience format.
func arrowRecordToMaps(record arrow.Record) ([]map[string]interface{}, error) {
	if record == nil {
		return []map[string]interface{}{}, nil
	}

	rows := make([]map[string]interface{}, record.NumRows())
	schema := record.Schema()

	for rowIdx := int64(0); rowIdx < record.NumRows(); rowIdx++ {
		row := make(map[string]interface{}, schema.NumFields())
		for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
			field := schema.Field(colIdx)
			col := record.Column(colIdx)
			row[field.Name] = arrowValueToInterface(col, int(rowIdx))
		}
		rows[rowIdx] = row
	}
	return rows, nil
}

// arrowValueToInterface extracts a single value from an Arrow array at the given index
func arrowValueToInterface(arr arrow.Array, idx int) interface{} {
	if arr.IsNull(idx) {
		return nil
	}

	switch a := arr.(type) {
	case *array.Int32:
		return a.Value(idx)
	case *array.Int64:
		return a.Value(idx)
	case *array.Float32:
		return float64(a.Value(idx))
	case *array.Float64:
		return a.Value(idx)
	case *array.String:
		return a.Value(idx)
	case *array.Boolean:
		return a.Value(idx)
	case *array.Binary:
		return a.Value(idx)
	case *array.FixedSizeList:
		values := a.ListValues()
		size := a.DataType().(*arrow.FixedSizeListType).Len()
		start := idx * int(size)
		if f32arr, ok := values.(*array.Float32); ok {
			result := make([]float32, size)
			for i := 0; i < int(size); i++ {
				result[i] = f32arr.Value(start + i)
			}
			return result
		}
		return nil
	default:
		return fmt.Sprintf("%v", arr.ValueStr(idx))
	}
}
