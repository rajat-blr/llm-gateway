package storage

import (
	"context"
	"time"
)

type UsageQuery struct {
	FromMS, ToMS           int64
	KeyID, Alias, Provider string
	Limit                  int
}
type RequestSummary struct {
	CacheHits    int64  `json:"cache_hits"`
	Coalesced    int64  `json:"coalesced_requests"`
	HourMS       int64  `json:"hour_ms"`
	KeyID        string `json:"key_id"`
	Alias        string `json:"alias"`
	Provider     string `json:"provider"`
	Outcome      string `json:"outcome"`
	Requests     int64  `json:"requests"`
	KnownUsage   int64  `json:"known_client_usage"`
	ClientInput  int64  `json:"client_input_tokens"`
	ClientOutput int64  `json:"client_output_tokens"`
	DurationMS   int64  `json:"total_duration_ms"`
}
type AttemptSummary struct {
	HourMS           int64  `json:"hour_ms"`
	KeyID            string `json:"key_id"`
	Alias            string `json:"alias"`
	Provider         string `json:"provider"`
	Outcome          string `json:"outcome"`
	Attempts         int64  `json:"attempts"`
	KnownUsage       int64  `json:"known_usage"`
	UnknownUsage     int64  `json:"unknown_usage"`
	Input            int64  `json:"input_tokens"`
	Output           int64  `json:"output_tokens"`
	Reserved         int64  `json:"reserved_tokens"`
	AccountingErrors int64  `json:"accounting_errors"`
}
type UsageReport struct {
	FromMS    int64            `json:"from_ms"`
	ToMS      int64            `json:"to_ms"`
	Requests  []RequestSummary `json:"requests"`
	Attempts  []AttemptSummary `json:"attempts"`
	Truncated bool             `json:"truncated"`
}

func (s *Store) Usage(ctx context.Context, q UsageQuery) (UsageReport, error) {
	report := UsageReport{FromMS: q.FromMS, ToMS: q.ToMS, Requests: []RequestSummary{}, Attempts: []AttemptSummary{}}
	if q.FromMS < 0 || q.ToMS <= q.FromMS || q.ToMS-q.FromMS > int64(31*24*time.Hour/time.Millisecond) || q.Limit < 1 || q.Limit > 1000 {
		return report, ErrInvalidQuery
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	params := []any{q.FromMS, q.ToMS, q.KeyID, q.KeyID, q.Alias, q.Alias, q.Provider, q.Provider, q.Limit + 1}
	rows, err := s.reader.QueryContext(ctx, `SELECT started_ms/3600000*3600000,key_id,alias,provider,outcome,COUNT(*),SUM(client_input IS NOT NULL),COALESCE(SUM(client_input),0),COALESCE(SUM(client_output),0),SUM(duration_ms),SUM(cache_state='HIT'),SUM(cache_state='COALESCED') FROM requests WHERE kind='client' AND started_ms>=? AND started_ms<? AND (?='' OR key_id=?) AND (?='' OR alias=?) AND (?='' OR provider=?) GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5 LIMIT ?`, params...)
	if err != nil {
		return report, ErrUnavailable
	}
	for rows.Next() {
		var r RequestSummary
		if rows.Scan(&r.HourMS, &r.KeyID, &r.Alias, &r.Provider, &r.Outcome, &r.Requests, &r.KnownUsage, &r.ClientInput, &r.ClientOutput, &r.DurationMS, &r.CacheHits, &r.Coalesced) != nil {
			rows.Close()
			return report, ErrUnavailable
		}
		report.Requests = append(report.Requests, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return report, ErrUnavailable
	}
	rows, err = s.reader.QueryContext(ctx, `SELECT a.started_ms/3600000*3600000,r.key_id,r.alias,a.provider,a.outcome,COUNT(*),SUM(a.input_tokens IS NOT NULL),SUM(a.input_tokens IS NULL),COALESCE(SUM(a.input_tokens),0),COALESCE(SUM(a.output_tokens),0),SUM(a.reserved_tokens),SUM(a.accounting_error) FROM attempts a JOIN requests r ON r.id=a.request_id WHERE a.started_ms>=? AND a.started_ms<? AND (?='' OR r.key_id=?) AND (?='' OR r.alias=?) AND (?='' OR a.provider=?) GROUP BY 1,2,3,4,5 ORDER BY 1,2,3,4,5 LIMIT ?`, params...)
	if err != nil {
		return report, ErrUnavailable
	}
	defer rows.Close()
	for rows.Next() {
		var r AttemptSummary
		if rows.Scan(&r.HourMS, &r.KeyID, &r.Alias, &r.Provider, &r.Outcome, &r.Attempts, &r.KnownUsage, &r.UnknownUsage, &r.Input, &r.Output, &r.Reserved, &r.AccountingErrors) != nil {
			return report, ErrUnavailable
		}
		report.Attempts = append(report.Attempts, r)
	}
	if rows.Err() != nil {
		return report, ErrUnavailable
	}
	if len(report.Requests) > q.Limit {
		report.Truncated = true
		report.Requests = report.Requests[:q.Limit]
	}
	if len(report.Attempts) > q.Limit {
		report.Truncated = true
		report.Attempts = report.Attempts[:q.Limit]
	}
	return report, nil
}
