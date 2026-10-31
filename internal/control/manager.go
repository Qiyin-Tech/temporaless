package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sync"
	"time"

	"temporaless"
	"temporaless/internal/backend"
	"temporaless/internal/registry"

	"go.temporal.io/sdk/temporal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var serviceIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,127}$`)

var ErrInvalidArgument = errors.New("invalid argument")

type Activity func(context.Context, temporaless.Request) (temporaless.Response, error)

type Worker interface {
	Stop()
}

type Runtime interface {
	Start(serviceID string, buildID string, activity Activity) (Worker, error)
	SetCurrent(ctx context.Context, serviceID string, buildID string) error
	Current(ctx context.Context, serviceID string) (string, error)
	Ping(ctx context.Context) error
}

type ManagerOptions struct {
	SwitchTimeout time.Duration
	RetryInterval time.Duration
}

type ServiceView struct {
	ID                  string                   `json:"id"`
	State               registry.ServiceState    `json:"state"`
	CurrentRevision     int64                    `json:"current_revision,omitempty"`
	RevisionState       registry.RevisionState   `json:"revision_state,omitempty"`
	RevisionKind        registry.RevisionKind    `json:"revision_kind,omitempty"`
	LatestRevision      int64                    `json:"latest_revision,omitempty"`
	LatestRevisionState registry.RevisionState   `json:"latest_revision_state,omitempty"`
	Spec                *temporaless.ServiceSpec `json:"spec,omitempty"`
	CreatedAt           time.Time                `json:"created_at"`
	UpdatedAt           time.Time                `json:"updated_at"`
}

type workerKey struct {
	serviceID string
	revision  int64
}

type Manager struct {
	registry *registry.Registry
	runtime  Runtime
	client   *http.Client
	options  ManagerOptions

	opMu    sync.Mutex
	workers map[workerKey]Worker
	closed  bool
}

func NewManager(store *registry.Registry, runtime Runtime, client *http.Client, options ManagerOptions) *Manager {
	if client == nil {
		client = http.DefaultClient
	}
	if options.SwitchTimeout <= 0 {
		options.SwitchTimeout = 30 * time.Second
	}
	if options.RetryInterval <= 0 {
		options.RetryInterval = 100 * time.Millisecond
	}
	return &Manager{
		registry: store,
		runtime:  runtime,
		client:   client,
		options:  options,
		workers:  make(map[workerKey]Worker),
	}
}

func (m *Manager) Put(ctx context.Context, serviceID string, spec temporaless.ServiceSpec) (ServiceView, error) {
	if err := validateServiceID(serviceID); err != nil {
		return ServiceView{}, err
	}
	compiled, err := backend.Compile(spec, m.client)
	if err != nil {
		return ServiceView{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}

	m.opMu.Lock()
	defer m.opMu.Unlock()
	if m.closed {
		return ServiceView{}, errors.New("manager is closed")
	}
	operationCtx, cancel := m.operationContext(ctx)
	defer cancel()
	if err := m.resumeStarting(operationCtx, serviceID); err != nil {
		return ServiceView{}, err
	}
	revision, noChange, err := m.registry.Prepare(operationCtx, serviceID, registry.KindService, compiled.SpecJSON(), compiled.Hash())
	if err != nil {
		return ServiceView{}, err
	}
	if !noChange {
		if err := m.applyRevision(operationCtx, revision, compiled.Invoke); err != nil {
			return ServiceView{}, err
		}
	}
	return m.getAfterOperation(serviceID)
}

func (m *Manager) Delete(ctx context.Context, serviceID string) (ServiceView, error) {
	if err := validateServiceID(serviceID); err != nil {
		return ServiceView{}, err
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if m.closed {
		return ServiceView{}, errors.New("manager is closed")
	}
	operationCtx, cancel := m.operationContext(ctx)
	defer cancel()
	if err := m.resumeStarting(operationCtx, serviceID); err != nil {
		return ServiceView{}, err
	}
	revision, noChange, err := m.registry.Prepare(operationCtx, serviceID, registry.KindTombstone, nil, "")
	if err != nil {
		return ServiceView{}, err
	}
	if !noChange {
		if err := m.applyRevision(operationCtx, revision, tombstoneActivity); err != nil {
			return ServiceView{}, err
		}
	}
	return m.getAfterOperation(serviceID)
}

func (m *Manager) Get(ctx context.Context, serviceID string) (ServiceView, error) {
	if err := validateServiceID(serviceID); err != nil {
		return ServiceView{}, err
	}
	return m.get(ctx, serviceID)
}

func (m *Manager) List(ctx context.Context) ([]ServiceView, error) {
	services, err := m.registry.List(ctx)
	if err != nil {
		return nil, err
	}
	views := make([]ServiceView, 0, len(services))
	for _, service := range services {
		view, err := serviceView(service)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, nil
}

func (m *Manager) Reconcile(ctx context.Context) error {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if m.closed {
		return errors.New("manager is closed")
	}

	services, err := m.registry.List(ctx)
	if err != nil {
		return err
	}
	reconcileErrors := m.prewarmCurrent(services)

	starting, err := m.registry.ListStarting(ctx)
	if err != nil {
		return errors.Join(append(reconcileErrors, err)...)
	}
	processed, startingErrors := m.reconcileStarting(ctx, starting)
	reconcileErrors = append(reconcileErrors, startingErrors...)
	reconcileErrors = append(reconcileErrors, m.restoreCurrent(ctx, services, processed)...)
	return errors.Join(reconcileErrors...)
}

func (m *Manager) prewarmCurrent(services []registry.Service) []error {
	var reconcileErrors []error
	for _, service := range services {
		if service.CurrentRevision == 0 {
			continue
		}
		activity, err := m.activityFor(service.Current)
		if err == nil {
			_, _, err = m.startWorker(service.Current, activity)
		}
		if err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("start current %s/r%d: %w", service.ID, service.CurrentRevision, err))
		}
	}
	return reconcileErrors
}

func (m *Manager) reconcileStarting(ctx context.Context, starting []registry.Revision) (map[string]struct{}, []error) {
	processed := make(map[string]struct{}, len(starting))
	var reconcileErrors []error
	for _, revision := range starting {
		operationCtx, cancel := m.operationContext(ctx)
		err := m.applyStoredRevision(operationCtx, revision)
		cancel()
		if err != nil {
			stored, stateErr := m.registry.GetRevision(ctx, revision.ServiceID, revision.Number)
			if stateErr == nil && stored.State == registry.RevisionFailed {
				continue
			}
			err = errors.Join(err, stateErr)
			reconcileErrors = append(reconcileErrors, fmt.Errorf("resume %s/r%d: %w", revision.ServiceID, revision.Number, err))
			if routingMustNotRollback(err) {
				processed[revision.ServiceID] = struct{}{}
			}
			continue
		}
		processed[revision.ServiceID] = struct{}{}
	}
	return processed, reconcileErrors
}

func (m *Manager) restoreCurrent(ctx context.Context, services []registry.Service, processed map[string]struct{}) []error {
	var reconcileErrors []error
	for _, service := range services {
		if service.CurrentRevision == 0 {
			continue
		}
		if _, ok := processed[service.ID]; ok {
			continue
		}
		operationCtx, cancel := m.operationContext(ctx)
		err := m.ensureCurrent(operationCtx, service.Current)
		cancel()
		if err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("restore %s/r%d: %w", service.ID, service.CurrentRevision, err))
		}
	}
	return reconcileErrors
}

func (m *Manager) Health(ctx context.Context) error {
	return errors.Join(m.registry.Ping(ctx), m.runtime.Ping(ctx))
}

func (m *Manager) Close() {
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if m.closed {
		return
	}
	m.closed = true
	for key, worker := range m.workers {
		worker.Stop()
		delete(m.workers, key)
	}
}

func (m *Manager) resumeStarting(ctx context.Context, serviceID string) error {
	revisions, err := m.registry.ListStarting(ctx)
	if err != nil {
		return err
	}
	for _, revision := range revisions {
		if revision.ServiceID == serviceID {
			if err := m.applyStoredRevision(ctx, revision); err != nil {
				return fmt.Errorf("resume r%d: %w", revision.Number, err)
			}
		}
	}
	return nil
}

func (m *Manager) applyStoredRevision(ctx context.Context, revision registry.Revision) error {
	activity, err := m.activityFor(revision)
	if err != nil {
		m.markFailed(revision)
		return err
	}
	return m.applyRevision(ctx, revision, activity)
}

func (m *Manager) ensureCurrent(ctx context.Context, revision registry.Revision) error {
	activity, err := m.activityFor(revision)
	if err != nil {
		return err
	}
	worker, _, err := m.startWorker(revision, activity)
	if err != nil {
		return err
	}
	if err := m.setCurrent(ctx, revision); err != nil {
		return err
	}
	m.workers[workerKey{serviceID: revision.ServiceID, revision: revision.Number}] = worker
	m.stopOtherWorkers(revision.ServiceID, revision.Number)
	return nil
}

func (m *Manager) applyRevision(ctx context.Context, revision registry.Revision, activity Activity) error {
	worker, started, err := m.startWorker(revision, activity)
	if err != nil {
		m.markFailed(revision)
		return err
	}
	if err := m.setCurrent(ctx, revision); err != nil {
		if isKnownSwitchFailure(err) {
			m.markFailed(revision)
			if started {
				m.stopWorker(revision.ServiceID, revision.Number)
			}
		}
		return err
	}
	stateCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	err = m.registry.Activate(stateCtx, revision.ServiceID, revision.Number)
	cancel()
	if err != nil {
		// Temporal is already routing here; keep this Worker alive for startup reconciliation.
		return &routingAppliedError{err: fmt.Errorf("activate registry after Temporal switch: %w", err)}
	}
	m.workers[workerKey{serviceID: revision.ServiceID, revision: revision.Number}] = worker
	m.stopOtherWorkers(revision.ServiceID, revision.Number)
	return nil
}

func (m *Manager) startWorker(revision registry.Revision, activity Activity) (Worker, bool, error) {
	key := workerKey{serviceID: revision.ServiceID, revision: revision.Number}
	if existing := m.workers[key]; existing != nil {
		return existing, false, nil
	}
	worker, err := m.runtime.Start(revision.ServiceID, buildID(revision.Number), activity)
	if err != nil {
		return nil, false, err
	}
	m.workers[key] = worker
	return worker, true, nil
}

type switchError struct {
	err   error
	known bool
}

func (e *switchError) Error() string { return e.err.Error() }
func (e *switchError) Unwrap() error { return e.err }

type routingAppliedError struct{ err error }

func (e *routingAppliedError) Error() string { return e.err.Error() }
func (e *routingAppliedError) Unwrap() error { return e.err }

func (m *Manager) setCurrent(ctx context.Context, revision registry.Revision) error {
	build := buildID(revision.Number)
	var lastErr error
	for {
		lastErr = m.runtime.SetCurrent(ctx, revision.ServiceID, build)
		if lastErr == nil {
			return nil
		}
		if !retryableSwitchError(lastErr) {
			return m.observeSwitchResult(revision, lastErr, nil)
		}
		select {
		case <-ctx.Done():
			return m.observeSwitchResult(revision, lastErr, ctx.Err())
		case <-time.After(m.options.RetryInterval):
		}
	}
}

func (m *Manager) observeSwitchResult(revision registry.Revision, switchErr, contextErr error) error {
	probeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	current, probeErr := m.runtime.Current(probeCtx, revision.ServiceID)
	cancel()
	if probeErr == nil && current == buildID(revision.Number) {
		return nil
	}
	return &switchError{
		err:   fmt.Errorf("set current version: %w", errors.Join(switchErr, contextErr, probeErr)),
		known: probeErr == nil,
	}
}

func isKnownSwitchFailure(err error) bool {
	var switchErr *switchError
	return errors.As(err, &switchErr) && switchErr.known
}

func routingMustNotRollback(err error) bool {
	var switchErr *switchError
	if errors.As(err, &switchErr) {
		return !switchErr.known
	}
	var appliedErr *routingAppliedError
	return errors.As(err, &appliedErr)
}

func retryableSwitchError(err error) bool {
	code := status.Code(err)
	var temporalStatus interface{ Status() *status.Status }
	if errors.As(err, &temporalStatus) {
		code = temporalStatus.Status().Code()
	}
	switch code {
	case codes.NotFound, codes.FailedPrecondition, codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}

func (m *Manager) activityFor(revision registry.Revision) (Activity, error) {
	if revision.Kind == registry.KindTombstone {
		return tombstoneActivity, nil
	}
	var spec temporaless.ServiceSpec
	if err := json.Unmarshal(revision.SpecJSON, &spec); err != nil {
		return nil, fmt.Errorf("decode stored spec: %w", err)
	}
	compiled, err := backend.Compile(spec, m.client)
	if err != nil {
		return nil, fmt.Errorf("compile stored spec: %w", err)
	}
	return compiled.Invoke, nil
}

func (m *Manager) stopOtherWorkers(serviceID string, keepRevision int64) {
	for key := range m.workers {
		if key.serviceID == serviceID && key.revision != keepRevision {
			m.stopWorker(key.serviceID, key.revision)
		}
	}
}

func (m *Manager) markFailed(revision registry.Revision) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.registry.MarkFailed(ctx, revision.ServiceID, revision.Number)
}

func (m *Manager) stopWorker(serviceID string, revision int64) {
	key := workerKey{serviceID: serviceID, revision: revision}
	if worker := m.workers[key]; worker != nil {
		worker.Stop()
		delete(m.workers, key)
	}
}

func (m *Manager) get(ctx context.Context, serviceID string) (ServiceView, error) {
	service, err := m.registry.Get(ctx, serviceID)
	if err != nil {
		return ServiceView{}, err
	}
	return serviceView(service)
}

func (m *Manager) getAfterOperation(serviceID string) (ServiceView, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return m.get(ctx, serviceID)
}

func serviceView(service registry.Service) (ServiceView, error) {
	view := ServiceView{
		ID:                  service.ID,
		State:               service.State,
		CurrentRevision:     service.CurrentRevision,
		RevisionState:       service.Current.State,
		RevisionKind:        service.Current.Kind,
		LatestRevision:      service.Latest.Number,
		LatestRevisionState: service.Latest.State,
		CreatedAt:           service.CreatedAt,
		UpdatedAt:           service.UpdatedAt,
	}
	if service.Current.Kind == registry.KindService {
		var spec temporaless.ServiceSpec
		if err := json.Unmarshal(service.Current.SpecJSON, &spec); err != nil {
			return ServiceView{}, err
		}
		view.Spec = &spec
	}
	return view, nil
}

func (m *Manager) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), m.options.SwitchTimeout)
}

func validateServiceID(serviceID string) error {
	if !serviceIDPattern.MatchString(serviceID) {
		return fmt.Errorf("%w: service id must match ^[a-z0-9][a-z0-9_-]{0,127}$", ErrInvalidArgument)
	}
	return nil
}

func buildID(revision int64) string {
	return fmt.Sprintf("r%d", revision)
}

func tombstoneActivity(context.Context, temporaless.Request) (temporaless.Response, error) {
	return temporaless.Response{}, temporal.NewNonRetryableApplicationError("service deleted", "ServiceDeleted", nil)
}
