package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

var (
	ErrNotFound     = errors.New("service not found")
	ErrInvalidState = errors.New("invalid revision state")
)

type ServiceState string

const (
	ServiceStarting ServiceState = "starting"
	ServiceActive   ServiceState = "active"
	ServiceDeleted  ServiceState = "deleted"
	ServiceFailed   ServiceState = "failed"
)

type RevisionKind string

const (
	KindService   RevisionKind = "service"
	KindTombstone RevisionKind = "tombstone"
)

type RevisionState string

const (
	RevisionStarting RevisionState = "starting"
	RevisionActive   RevisionState = "active"
	RevisionDraining RevisionState = "draining"
	RevisionFailed   RevisionState = "failed"
)

type Revision struct {
	ServiceID     string
	Number        int64
	Kind          RevisionKind
	State         RevisionState
	SwitchStarted bool
	SpecJSON      []byte
	Hash          string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Service struct {
	ID              string
	State           ServiceState
	CurrentRevision int64
	Current         Revision
	Latest          Revision
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type Registry struct {
	db *sql.DB
}

func Open(path string) (*Registry, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	registry := &Registry{db: db}
	if err := registry.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return registry, nil
}

func (r *Registry) Close() error {
	return r.db.Close()
}

func (r *Registry) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

func (r *Registry) Prepare(ctx context.Context, serviceID string, kind RevisionKind, specJSON []byte, hash string) (Revision, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return Revision{}, false, err
	}
	defer tx.Rollback()

	position, err := loadServicePosition(ctx, tx, serviceID)
	if err != nil {
		return Revision{}, false, err
	}
	current, noChange, err := decidePrepare(position, kind, hash)
	if err != nil || noChange {
		return current, noChange, err
	}

	now := time.Now().UTC()
	if !position.exists {
		if err := insertService(ctx, tx, serviceID, now); err != nil {
			return Revision{}, false, err
		}
	}
	revision, err := insertStartingRevision(ctx, tx, serviceID, kind, specJSON, hash, now)
	if err != nil {
		return Revision{}, false, err
	}
	return revision, false, tx.Commit()
}

func decidePrepare(position servicePosition, kind RevisionKind, hash string) (Revision, bool, error) {
	if kind == KindTombstone && !position.exists {
		return Revision{}, false, ErrNotFound
	}
	current, noChange := position.noChange(kind, hash)
	return current, noChange, nil
}

type servicePosition struct {
	exists  bool
	state   ServiceState
	current *Revision
}

func loadServicePosition(ctx context.Context, tx *sql.Tx, serviceID string) (servicePosition, error) {
	var position servicePosition
	var current sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT state, current_revision FROM services WHERE id = ?`, serviceID).Scan(&position.state, &current)
	if errors.Is(err, sql.ErrNoRows) {
		return position, nil
	}
	if err != nil {
		return servicePosition{}, err
	}
	position.exists = true
	if current.Valid {
		revision, err := queryRevision(ctx, tx, serviceID, current.Int64)
		if err != nil {
			return servicePosition{}, err
		}
		position.current = &revision
	}
	return position, nil
}

func (p servicePosition) noChange(kind RevisionKind, hash string) (Revision, bool) {
	if p.current == nil {
		return Revision{}, false
	}
	if kind == KindTombstone && p.state == ServiceDeleted {
		return *p.current, true
	}
	if kind == KindService && p.state == ServiceActive && p.current.Kind == KindService && p.current.Hash == hash {
		return *p.current, true
	}
	return Revision{}, false
}

func insertService(ctx context.Context, tx *sql.Tx, serviceID string, now time.Time) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO services (id, state, current_revision, created_at, updated_at) VALUES (?, ?, NULL, ?, ?)`,
		serviceID, ServiceStarting, formatTime(now), formatTime(now))
	return err
}

func insertStartingRevision(ctx context.Context, tx *sql.Tx, serviceID string, kind RevisionKind, specJSON []byte, hash string, now time.Time) (Revision, error) {
	var number int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(revision), 0) + 1 FROM service_revisions WHERE service_id = ?`, serviceID).Scan(&number); err != nil {
		return Revision{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO service_revisions
			(service_id, revision, kind, state, switch_started, spec_json, config_hash, created_at, updated_at)
		VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)`,
		serviceID, number, kind, RevisionStarting, specJSON, hash, formatTime(now), formatTime(now)); err != nil {
		return Revision{}, err
	}
	return Revision{
		ServiceID: serviceID,
		Number:    number,
		Kind:      kind,
		State:     RevisionStarting,
		SpecJSON:  append([]byte(nil), specJSON...),
		Hash:      hash,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

func (r *Registry) MarkSwitchStarted(ctx context.Context, serviceID string, number int64) error {
	now := formatTime(time.Now().UTC())
	result, err := r.db.ExecContext(ctx,
		`UPDATE service_revisions SET switch_started = 1, updated_at = ? WHERE service_id = ? AND revision = ? AND state = ?`,
		now, serviceID, number, RevisionStarting)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return ErrInvalidState
	}
	return nil
}

func (r *Registry) Activate(ctx context.Context, serviceID string, number int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	revision, err := queryRevision(ctx, tx, serviceID, number)
	if err != nil {
		return err
	}
	if revision.State != RevisionStarting && revision.State != RevisionActive {
		return fmt.Errorf("%w: cannot activate %s", ErrInvalidState, revision.State)
	}
	now := formatTime(time.Now().UTC())
	if _, err := tx.ExecContext(ctx,
		`UPDATE service_revisions SET state = ?, updated_at = ? WHERE service_id = ? AND state = ? AND revision <> ?`,
		RevisionDraining, now, serviceID, RevisionActive, number); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE service_revisions SET state = ?, updated_at = ? WHERE service_id = ? AND revision = ?`,
		RevisionActive, now, serviceID, number); err != nil {
		return err
	}
	serviceState := activeServiceState(revision.Kind)
	if _, err := tx.ExecContext(ctx,
		`UPDATE services SET state = ?, current_revision = ?, updated_at = ? WHERE id = ?`,
		serviceState, number, now, serviceID); err != nil {
		return err
	}
	return tx.Commit()
}

func activeServiceState(kind RevisionKind) ServiceState {
	if kind == KindTombstone {
		return ServiceDeleted
	}
	return ServiceActive
}

func (r *Registry) MarkFailed(ctx context.Context, serviceID string, number int64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := formatTime(time.Now().UTC())
	result, err := tx.ExecContext(ctx,
		`UPDATE service_revisions SET state = ?, updated_at = ? WHERE service_id = ? AND revision = ? AND state = ?`,
		RevisionFailed, now, serviceID, number, RevisionStarting)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return ErrInvalidState
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE services SET state = ?, updated_at = ? WHERE id = ? AND current_revision IS NULL`,
		ServiceFailed, now, serviceID); err != nil {
		return err
	}
	return tx.Commit()
}

func (r *Registry) Get(ctx context.Context, serviceID string) (Service, error) {
	service, current, err := r.getServiceRow(ctx, serviceID)
	if err != nil {
		return Service{}, err
	}
	if err := r.loadServiceRevisions(ctx, &service, current); err != nil {
		return Service{}, err
	}
	return service, nil
}

func (r *Registry) getServiceRow(ctx context.Context, serviceID string) (Service, sql.NullInt64, error) {
	var service Service
	var current sql.NullInt64
	var createdAt, updatedAt string
	err := r.db.QueryRowContext(ctx,
		`SELECT id, state, current_revision, created_at, updated_at FROM services WHERE id = ?`, serviceID,
	).Scan(&service.ID, &service.State, &current, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Service{}, current, ErrNotFound
	}
	if err != nil {
		return Service{}, current, err
	}
	service.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return Service{}, current, err
	}
	service.UpdatedAt, err = parseTime(updatedAt)
	if err != nil {
		return Service{}, current, err
	}
	return service, current, nil
}

func (r *Registry) loadServiceRevisions(ctx context.Context, service *Service, current sql.NullInt64) error {
	var err error
	if current.Valid {
		service.CurrentRevision = current.Int64
		service.Current, err = r.GetRevision(ctx, service.ID, current.Int64)
		if err != nil {
			return err
		}
	}
	var latest int64
	if err := r.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(revision), 0) FROM service_revisions WHERE service_id = ?`, service.ID,
	).Scan(&latest); err != nil {
		return err
	}
	if latest > 0 {
		service.Latest, err = r.GetRevision(ctx, service.ID, latest)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Registry) List(ctx context.Context) ([]Service, error) {
	rows, err := r.db.QueryContext(ctx, `
		WITH latest_revisions AS (
			SELECT service_id, MAX(revision) AS revision
			FROM service_revisions
			GROUP BY service_id
		)
		SELECT
			s.id, s.state, s.current_revision, s.created_at, s.updated_at,
			current.revision, current.kind, current.state, current.switch_started,
			current.spec_json, current.config_hash, current.created_at, current.updated_at,
			latest.revision, latest.kind, latest.state, latest.switch_started,
			latest.spec_json, latest.config_hash, latest.created_at, latest.updated_at
		FROM services AS s
		LEFT JOIN service_revisions AS current
			ON current.service_id = s.id AND current.revision = s.current_revision
		LEFT JOIN latest_revisions
			ON latest_revisions.service_id = s.id
		LEFT JOIN service_revisions AS latest
			ON latest.service_id = latest_revisions.service_id
			AND latest.revision = latest_revisions.revision
		ORDER BY s.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var services []Service
	for rows.Next() {
		var service Service
		var currentNumber sql.NullInt64
		var serviceCreatedAt, serviceUpdatedAt string
		var current, latest joinedRevision
		if err := rows.Scan(
			&service.ID, &service.State, &currentNumber, &serviceCreatedAt, &serviceUpdatedAt,
			&current.number, &current.kind, &current.state, &current.switchStarted,
			&current.specJSON, &current.hash, &current.createdAt, &current.updatedAt,
			&latest.number, &latest.kind, &latest.state, &latest.switchStarted,
			&latest.specJSON, &latest.hash, &latest.createdAt, &latest.updatedAt,
		); err != nil {
			return nil, err
		}
		var err error
		service.CreatedAt, err = parseTime(serviceCreatedAt)
		if err != nil {
			return nil, err
		}
		service.UpdatedAt, err = parseTime(serviceUpdatedAt)
		if err != nil {
			return nil, err
		}
		if currentNumber.Valid {
			service.CurrentRevision = currentNumber.Int64
			service.Current, err = current.revision(service.ID)
			if err != nil {
				return nil, err
			}
		}
		if latest.number.Valid {
			service.Latest, err = latest.revision(service.ID)
			if err != nil {
				return nil, err
			}
		}
		services = append(services, service)
	}
	return services, rows.Err()
}

type joinedRevision struct {
	number        sql.NullInt64
	kind          sql.NullString
	state         sql.NullString
	switchStarted sql.NullInt64
	specJSON      []byte
	hash          sql.NullString
	createdAt     sql.NullString
	updatedAt     sql.NullString
}

func (r joinedRevision) revision(serviceID string) (Revision, error) {
	if !r.number.Valid {
		return Revision{}, ErrNotFound
	}
	createdAt, err := parseTime(r.createdAt.String)
	if err != nil {
		return Revision{}, err
	}
	updatedAt, err := parseTime(r.updatedAt.String)
	if err != nil {
		return Revision{}, err
	}
	return Revision{
		ServiceID:     serviceID,
		Number:        r.number.Int64,
		Kind:          RevisionKind(r.kind.String),
		State:         RevisionState(r.state.String),
		SwitchStarted: r.switchStarted.Int64 != 0,
		SpecJSON:      r.specJSON,
		Hash:          r.hash.String,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}, nil
}

func (r *Registry) ListStarting(ctx context.Context) ([]Revision, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT service_id, revision, kind, state, switch_started, spec_json, config_hash, created_at, updated_at
		FROM service_revisions WHERE state = ? ORDER BY service_id, revision`, RevisionStarting)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var revisions []Revision
	for rows.Next() {
		revision, err := scanRevision(rows)
		if err != nil {
			return nil, err
		}
		revisions = append(revisions, revision)
	}
	return revisions, rows.Err()
}

func (r *Registry) GetRevision(ctx context.Context, serviceID string, number int64) (Revision, error) {
	return queryRevision(ctx, r.db, serviceID, number)
}

func (r *Registry) migrate(ctx context.Context) error {
	if _, err := r.db.ExecContext(ctx, `PRAGMA foreign_keys = ON; PRAGMA busy_timeout = 5000;`); err != nil {
		return err
	}
	_, err := r.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS services (
			id TEXT PRIMARY KEY,
			state TEXT NOT NULL CHECK (state IN ('starting', 'active', 'deleted', 'failed')),
			current_revision INTEGER,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS service_revisions (
			service_id TEXT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
			revision INTEGER NOT NULL,
			kind TEXT NOT NULL CHECK (kind IN ('service', 'tombstone')),
			state TEXT NOT NULL CHECK (state IN ('starting', 'active', 'draining', 'failed')),
			switch_started INTEGER NOT NULL DEFAULT 0 CHECK (switch_started IN (0, 1)),
			spec_json BLOB,
			config_hash TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (service_id, revision)
		);
		CREATE INDEX IF NOT EXISTS service_revisions_state ON service_revisions(state);
	`)
	if err != nil {
		return err
	}
	hasSwitchStarted, err := r.hasColumn(ctx, "service_revisions", "switch_started")
	if err != nil || hasSwitchStarted {
		return err
	}
	if _, err := r.db.ExecContext(ctx, `ALTER TABLE service_revisions ADD COLUMN switch_started INTEGER NOT NULL DEFAULT 0 CHECK (switch_started IN (0, 1))`); err != nil {
		return err
	}
	// Existing starting rows may already have an in-flight Temporal update.
	_, err = r.db.ExecContext(ctx, `UPDATE service_revisions SET switch_started = 1 WHERE state = ?`, RevisionStarting)
	return err
}

func (r *Registry) hasColumn(ctx context.Context, table, column string) (bool, error) {
	rows, err := r.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func queryRevision(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, serviceID string, number int64) (Revision, error) {
	return scanRevision(queryer.QueryRowContext(ctx, `
		SELECT service_id, revision, kind, state, switch_started, spec_json, config_hash, created_at, updated_at
		FROM service_revisions WHERE service_id = ? AND revision = ?`, serviceID, number))
}

func scanRevision(row scanner) (Revision, error) {
	var revision Revision
	var switchStarted int
	var createdAt, updatedAt string
	if err := row.Scan(
		&revision.ServiceID,
		&revision.Number,
		&revision.Kind,
		&revision.State,
		&switchStarted,
		&revision.SpecJSON,
		&revision.Hash,
		&createdAt,
		&updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Revision{}, ErrNotFound
		}
		return Revision{}, err
	}
	revision.SwitchStarted = switchStarted != 0
	var err error
	revision.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return Revision{}, err
	}
	revision.UpdatedAt, err = parseTime(updatedAt)
	if err != nil {
		return Revision{}, err
	}
	return revision, nil
}

func formatTime(value time.Time) string {
	return value.Format(time.RFC3339Nano)
}

func parseTime(value string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, value)
}
