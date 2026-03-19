// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

package tests

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lancedb/lancedb-go/pkg/internal"
	"github.com/lancedb/lancedb-go/pkg/lancedb"
)

// setupQueryTestTable creates a test table with sample data for query builder tests.
// Returns the table and a cleanup function.
func setupQueryTestTable(t *testing.T) (*internal.Table, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "lancedb_test_query_builder_")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	conn, err := lancedb.Connect(context.Background(), tempDir, nil)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to connect: %v", err)
	}

	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	schema, err := internal.NewSchema(arrowSchema)
	if err != nil {
		conn.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create schema: %v", err)
	}

	table, err := conn.CreateTable(context.Background(), "test_qb", schema)
	if err != nil {
		conn.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create table: %v", err)
	}

	pool := memory.NewGoAllocator()
	idBuilder := array.NewInt32Builder(pool)
	idBuilder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	nameBuilder := array.NewStringBuilder(pool)
	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie", "Diana", "Eve"}, nil)
	nameArray := nameBuilder.NewArray()
	defer nameArray.Release()

	scoreBuilder := array.NewFloat64Builder(pool)
	scoreBuilder.AppendValues([]float64{95.5, 87.2, 92.8, 88.9, 94.1}, nil)
	scoreArray := scoreBuilder.NewArray()
	defer scoreArray.Release()

	columns := []arrow.Array{idArray, nameArray, scoreArray}
	record := array.NewRecord(arrowSchema, columns, 5)
	defer record.Release()

	err = table.Add(context.Background(), record, nil)
	if err != nil {
		table.Close()
		conn.Close()
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to add data: %v", err)
	}

	cleanup := func() {
		table.Close()
		conn.Close()
		os.RemoveAll(tempDir)
	}

	return table.(*internal.Table), cleanup
}

func TestQueryBuilderExecute(t *testing.T) {
	table, cleanup := setupQueryTestTable(t)
	defer cleanup()

	t.Run("Execute with no options returns all rows", func(t *testing.T) {
		results, err := table.Query().Execute()
		require.NoError(t, err)
		assert.Len(t, results, 5)
	})

	t.Run("Execute with filter", func(t *testing.T) {
		results, err := table.Query().Filter("score > 90").Execute()
		require.NoError(t, err)
		assert.Len(t, results, 3)
		for _, row := range results {
			score, ok := row["score"].(float64)
			require.True(t, ok, "Score should be float64")
			assert.Greater(t, score, 90.0)
		}
	})

	t.Run("Execute with multiple filters joined by AND", func(t *testing.T) {
		results, err := table.Query().
			Filter("score > 85").
			Filter("score < 95").
			Execute()
		require.NoError(t, err)
		assert.Len(t, results, 4)
		for _, row := range results {
			score, ok := row["score"].(float64)
			require.True(t, ok, "Score should be float64")
			assert.Greater(t, score, 85.0)
			assert.Less(t, score, 95.0)
		}
	})

	t.Run("Execute with limit", func(t *testing.T) {
		results, err := table.Query().Limit(2).Execute()
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("Execute with offset", func(t *testing.T) {
		results, err := table.Query().Offset(2).Execute()
		require.NoError(t, err)
		assert.Len(t, results, 3)
	})

	t.Run("Execute with columns", func(t *testing.T) {
		results, err := table.Query().Columns([]string{"id", "name"}).Execute()
		require.NoError(t, err)
		assert.Len(t, results, 5)
		for i, row := range results {
			assert.Len(t, row, 2, "Record %d should have 2 columns", i)
			assert.Contains(t, row, "id")
			assert.Contains(t, row, "name")
		}
	})

	t.Run("Execute with filter and limit chained", func(t *testing.T) {
		results, err := table.Query().Filter("score > 85").Limit(2).Execute()
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("Execute on closed table returns error", func(t *testing.T) {
		closedTable, closedCleanup := setupQueryTestTable(t)
		closedTable.Close()
		defer closedCleanup()

		_, err := closedTable.Query().Execute()
		assert.Error(t, err)
	})
}

func TestQueryBuilderExecuteAsync(t *testing.T) {
	table, cleanup := setupQueryTestTable(t)
	defer cleanup()

	t.Run("ExecuteAsync returns results on channel", func(t *testing.T) {
		resultChan, errChan := table.Query().Filter("score > 90").ExecuteAsync()

		select {
		case results := <-resultChan:
			assert.Len(t, results, 3)
		case err := <-errChan:
			t.Fatalf("ExecuteAsync failed: %v", err)
		}
	})

	t.Run("ExecuteAsync on closed table returns error", func(t *testing.T) {
		closedTable, closedCleanup := setupQueryTestTable(t)
		closedTable.Close()
		defer closedCleanup()

		resultChan, errChan := closedTable.Query().ExecuteAsync()

		select {
		case <-resultChan:
			t.Fatal("Expected error, got results")
		case err := <-errChan:
			assert.Error(t, err)
		}
	})
}
