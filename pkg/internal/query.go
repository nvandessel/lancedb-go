// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

package internal

import (
	"context"
	"fmt"
	"strings"

	lancedb "github.com/lancedb/lancedb-go/pkg/contracts"
)

// QueryBuilder provides a fluent interface for building queries
type QueryBuilder struct {
	table   *Table
	filters []string
	limit   int
	offset  int
	columns []string
}

var _ lancedb.IQueryBuilder = (*QueryBuilder)(nil)
var _ lancedb.IVectorQueryBuilder = (*VectorQueryBuilder)(nil)

// VectorQueryBuilder extends QueryBuilder for vector similarity searches
type VectorQueryBuilder struct {
	QueryBuilder
	vector []float32
	column string
}

// Filter adds a filter condition to the query
func (q *QueryBuilder) Filter(condition string) lancedb.IQueryBuilder {
	q.filters = append(q.filters, condition)
	return q
}

// Limit sets the maximum number of results to return
func (q *QueryBuilder) Limit(limit int) lancedb.IQueryBuilder {
	q.limit = limit
	return q
}

// Columns sets the columns to return
func (q *QueryBuilder) Columns(columns []string) lancedb.IQueryBuilder {
	q.columns = columns
	return q
}

// Offset sets the number of rows to skip
func (q *QueryBuilder) Offset(offset int) lancedb.IQueryBuilder {
	q.offset = offset
	return q
}

// Execute executes the query and returns results.
// Delegates to Table.Select() which holds the mutex and checks closed state.
func (q *QueryBuilder) Execute() ([]map[string]interface{}, error) {
	config := q.buildConfig()
	return q.table.Select(context.Background(), config)
}

// ExecuteAsync executes the query asynchronously
func (q *QueryBuilder) ExecuteAsync() (<-chan []map[string]interface{}, <-chan error) {
	resultChan := make(chan []map[string]interface{}, 1)
	errorChan := make(chan error, 1)

	go func() {
		results, err := q.Execute()
		if err != nil {
			errorChan <- err
		} else {
			resultChan <- results
		}
		close(resultChan)
		close(errorChan)
	}()

	return resultChan, errorChan
}

// ApplyOptions applies query options to the builder
func (q *QueryBuilder) ApplyOptions(options *lancedb.QueryOptions) lancedb.IQueryBuilder {
	if options != nil {
		if options.MaxResults > 0 {
			q.Limit(options.MaxResults)
		}
	}
	return q
}

// buildConfig converts the builder's accumulated state into a QueryConfig
func (q *QueryBuilder) buildConfig() lancedb.QueryConfig {
	config := lancedb.QueryConfig{}

	if len(q.filters) > 0 {
		config.Where = strings.Join(q.filters, " AND ")
	}
	if q.limit > 0 {
		limit := q.limit
		config.Limit = &limit
	}
	if q.offset > 0 {
		offset := q.offset
		config.Offset = &offset
	}
	if len(q.columns) > 0 {
		config.Columns = q.columns
	}

	return config
}

// Filter adds a filter condition to the vector query
func (vq *VectorQueryBuilder) Filter(condition string) lancedb.IVectorQueryBuilder {
	vq.QueryBuilder.Filter(condition)
	return vq
}

// Limit sets the maximum number of results to return
func (vq *VectorQueryBuilder) Limit(limit int) lancedb.IVectorQueryBuilder {
	vq.QueryBuilder.Limit(limit)
	return vq
}

// Columns sets the columns to return
func (vq *VectorQueryBuilder) Columns(columns []string) lancedb.IVectorQueryBuilder {
	vq.QueryBuilder.Columns(columns)
	return vq
}

// Offset sets the number of rows to skip
func (vq *VectorQueryBuilder) Offset(offset int) lancedb.IVectorQueryBuilder {
	vq.QueryBuilder.Offset(offset)
	return vq
}

// Execute executes the vector search query and returns results.
// Delegates to Table.Select() which holds the mutex and checks closed state.
func (vq *VectorQueryBuilder) Execute() ([]map[string]interface{}, error) {
	if len(vq.vector) == 0 {
		return nil, fmt.Errorf("vector search requires a non-empty query vector")
	}
	if vq.column == "" {
		return nil, fmt.Errorf("vector search requires a non-empty column name")
	}
	k := vq.limit
	if k <= 0 {
		return nil, fmt.Errorf("vector search requires a positive K value: call .Limit(k) before .Execute()")
	}

	config := vq.buildConfig()
	config.VectorSearch = &lancedb.VectorSearch{
		Column: vq.column,
		Vector: vq.vector,
		K:      k,
	}

	return vq.table.Select(context.Background(), config)
}

// ExecuteAsync executes the vector query asynchronously
func (vq *VectorQueryBuilder) ExecuteAsync() (<-chan []map[string]interface{}, <-chan error) {
	resultChan := make(chan []map[string]interface{}, 1)
	errorChan := make(chan error, 1)

	go func() {
		results, err := vq.Execute()
		if err != nil {
			errorChan <- err
		} else {
			resultChan <- results
		}
		close(resultChan)
		close(errorChan)
	}()

	return resultChan, errorChan
}

// ApplyOptions applies query options to the vector query builder
func (vq *VectorQueryBuilder) ApplyOptions(options *lancedb.QueryOptions) lancedb.IVectorQueryBuilder {
	vq.QueryBuilder.ApplyOptions(options)
	return vq
}
