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

// ErrInvalidArgument marks invalid service IDs and service configurations.
var ErrInvalidArgument = errors.New("invalid argument")

// Activity is the function signature registered with a Temporal Activity Worker.
type Activity func(context.Context, temporaless.Request) (temporaless.Response, error)

// Worker is the lifecycle surface Manager needs from a Temporal Worker.
type Worker interface {
	Stop()
}

// Runtime starts revision Workers and controls Worker Deployment routing.
type Runtime interface {
	Start(serviceID string, buildID string, activity Activity) (Worker, error)
	SetCurrent(ctx context.Context, serviceID string, buildID string) error
	Current(ctx context.Context, serviceID string) (string, error)
	Ping(ctx context.Context) error
}

// ManagerOptions controls revision switching and backend response limits.
type ManagerOptions struct {
	SwitchTimeout               time.Duration
	RetryInterval               time.Duration
	MaxBackendResponseBodyBytes int64
}

// ServiceView is the REST representation of a service and its revision state.
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

// ServiceHealthView is the REST result of one optional backend health probe.
type ServiceHealthView struct {
	ID            string                `json:"service_id"`
	Revision      int64                 `json:"revision,omitempty"`
	RegistryState registry.ServiceState `json:"registry_state"`
	Health        string                `json:"health"`
	HTTPStatus    int                   `json:"http_status,omitempty"`
	LatencyMS     float64               `json:"latency_ms,omitempty"`
	Message       string                `json:"message,omitempty"`
	CheckedAt     time.Time             `json:"checked_at"`
}

type workerKey struct {
	serviceID string
	revision  int64
}

// Manager serializes control-plane changes and reconciles SQLite with Temporal routing.
type Manager struct {
	registry *registry.Registry
	runtime  Runtime
	client   *http.Client
	options  ManagerOptions

	opMu      sync.Mutex
	workers   map[workerKey]Worker
	closed    bool
	closeDone chan struct{}
}

// NewManager creates a control-plane manager using shared Registry, Runtime, and HTTP client instances.
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
	if options.MaxBackendResponseBodyBytes <= 0 {
		options.MaxBackendResponseBodyBytes = backend.DefaultMaxResponseBodyBytes
	}
	return &Manager{
		registry:  store,
		runtime:   runtime,
		client:    client,
		options:   options,
		workers:   make(map[workerKey]Worker),
		closeDone: make(chan struct{}),
	}
}

// Put creates, updates, or restores a service and makes its new revision current.
func (m *Manager) Put(ctx context.Context, serviceID string, spec temporaless.ServiceSpec) (ServiceView, error) {
	if err := validateServiceID(serviceID); err != nil {
		return ServiceView{}, err
	}
	compiled, err := backend.Compile(spec, m.client, m.options.MaxBackendResponseBodyBytes)
	if err != nil {
		return ServiceView{}, fmt.Errorf("%w: %v", ErrInvalidArgument, err)
	}

	m.opMu.Lock()
	defer m.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return ServiceView{}, err
	}
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

// Delete creates and activates a tombstone revision for a service.
func (m *Manager) Delete(ctx context.Context, serviceID string) (ServiceView, error) {
	if err := validateServiceID(serviceID); err != nil {
		return ServiceView{}, err
	}
	m.opMu.Lock()
	defer m.opMu.Unlock()
	if err := ctx.Err(); err != nil {
		return ServiceView{}, err
	}
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

// Get returns one service and its current and latest revision state.
func (m *Manager) Get(ctx context.Context, serviceID string) (ServiceView, error) {
	if err := validateServiceID(serviceID); err != nil {
		return ServiceView{}, err
	}
	return m.get(ctx, serviceID)
}

// List returns all services ordered by service ID.
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

// ServiceHealth runs the current revision's optional direct HTTP health probe.
func (m *Manager) ServiceHealth(ctx context.Context, serviceID string) (ServiceHealthView, error) {
	if err := validateServiceID(serviceID); err != nil {
		return ServiceHealthView{}, err
	}
	service, err := m.registry.Get(ctx, serviceID)
	if err != nil {
		return ServiceHealthView{}, err
	}
	view := ServiceHealthView{
		ID:            service.ID,
		Revision:      service.CurrentRevision,
		RegistryState: service.State,
		CheckedAt:     time.Now().UTC(),
	}
	if service.State != registry.ServiceActive || service.Current.Kind != registry.KindService {
		view.Health = "not_ready"
		view.Message = fmt.Sprintf("service state is %s", service.State)
		return view, nil
	}
	var spec temporaless.ServiceSpec
	if err := json.Unmarshal(service.Current.SpecJSON, &spec); err != nil {
		return ServiceHealthView{}, fmt.Errorf("decode stored spec: %w", err)
	}
	if spec.Health == nil {
		view.Health = "not_configured"
		return view, nil
	}
	probe, err := backend.NewHealthProbe(*spec.Health, m.client)
	if err != nil {
		return ServiceHealthView{}, fmt.Errorf("compile stored health spec: %w", err)
	}
	result, probeErr := probe.Check(ctx)
	view.CheckedAt = time.Now().UTC()
	view.HTTPStatus = result.Status
	view.LatencyMS = float64(result.Latency) / float64(time.Millisecond)
	if probeErr != nil {
		view.Health = "unhealthy"
		view.Message = probeErr.Error()
		return view, nil
	}
	view.Message = result.Message
	if result.Status >= http.StatusOK && result.Status < http.StatusMultipleChoices {
		view.Health = "healthy"
	} else {
		view.Health = "unhealthy"
	}
	return view, nil
}

// Reconcile restores current Workers and resumes interrupted revision switches after startup.
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
	prewarmErrors := m.prewarmCurrent(services)

	starting, err := m.registry.ListStarting(ctx)
	if err != nil {
		return errors.Join(append(prewarmErrorValues(prewarmErrors, nil), err)...)
	}
	processed, activated, reconcileErrors := m.reconcileStarting(ctx, starting)
	reconcileErrors = append(prewarmErrorValues(prewarmErrors, activated), reconcileErrors...)
	reconcileErrors = append(reconcileErrors, m.restoreCurrent(ctx, services, processed)...)
	return errors.Join(reconcileErrors...)
}

func (m *Manager) prewarmCurrent(services []registry.Service) map[string]error {
	reconcileErrors := make(map[string]error)
	for _, service := range services {
		if service.CurrentRevision == 0 {
			continue
		}
		activity, err := m.activityFor(service.Current)
		if err == nil {
			_, _, err = m.startWorker(service.Current, activity)
		}
		if err != nil {
			reconcileErrors[service.ID] = fmt.Errorf("start current %s/r%d: %w", service.ID, service.CurrentRevision, err)
		}
	}
	return reconcileErrors
}

func prewarmErrorValues(failures map[string]error, replaced map[string]struct{}) []error {
	result := make([]error, 0, len(failures))
	for serviceID, err := range failures {
		if _, ok := replaced[serviceID]; !ok {
			result = append(result, err)
		}
	}
	return result
}

func (m *Manager) reconcileStarting(ctx context.Context, starting []registry.Revision) (map[string]struct{}, map[string]struct{}, []error) {
	processed := make(map[string]struct{}, len(starting))
	activated := make(map[string]struct{}, len(starting))
	var reconcileErrors []error
	for _, revision := range starting {
		operationCtx, cancel := m.reconcileOperationContext(ctx)
		err := m.applyStoredRevision(operationCtx, revision)
		cancel()
		if err != nil {
			stored, stateErr := m.registry.GetRevision(ctx, revision.ServiceID, revision.Number)
			if stateErr == nil && stored.State == registry.RevisionFailed {
				continue
			}
			err = errors.Join(err, stateErr)
			reconcileErrors = append(reconcileErrors, fmt.Errorf("resume %s/r%d: %w", revision.ServiceID, revision.Number, err))
			if revision.SwitchStarted || routingMustNotRollback(err) {
				processed[revision.ServiceID] = struct{}{}
			}
			continue
		}
		processed[revision.ServiceID] = struct{}{}
		activated[revision.ServiceID] = struct{}{}
	}
	return processed, activated, reconcileErrors
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
		operationCtx, cancel := m.reconcileOperationContext(ctx)
		err := m.ensureCurrent(operationCtx, service.Current)
		cancel()
		if err != nil {
			reconcileErrors = append(reconcileErrors, fmt.Errorf("restore %s/r%d: %w", service.ID, service.CurrentRevision, err))
		}
	}
	return reconcileErrors
}

// Health checks SQLite and Temporal Frontend connectivity.
func (m *Manager) Health(ctx context.Context) error {
	return errors.Join(m.registry.Ping(ctx), m.runtime.Ping(ctx))
}

// Close concurrently drains and stops every managed Worker.
func (m *Manager) Close() {
	m.opMu.Lock()
	if m.closed {
		done := m.closeDone
		m.opMu.Unlock()
		<-done
		return
	}
	m.closed = true
	workers := make([]Worker, 0, len(m.workers))
	for key, worker := range m.workers {
		workers = append(workers, worker)
		delete(m.workers, key)
	}
	done := m.closeDone
	m.opMu.Unlock()

	var stops sync.WaitGroup
	stops.Add(len(workers))
	for _, worker := range workers {
		go func() {
			defer stops.Done()
			worker.Stop()
		}()
	}
	stops.Wait()
	close(done)
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
		if !revision.SwitchStarted {
			m.markFailed(revision)
		}
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
	if err := m.setCurrent(ctx, revision, false); err != nil {
		return err
	}
	m.workers[workerKey{serviceID: revision.ServiceID, revision: revision.Number}] = worker
	m.stopOtherWorkers(revision.ServiceID, revision.Number)
	return nil
}

func (m *Manager) applyRevision(ctx context.Context, revision registry.Revision, activity Activity) error {
	priorSwitchAttempt := revision.SwitchStarted
	worker, started, err := m.startWorker(revision, activity)
	if err != nil {
		if !priorSwitchAttempt {
			m.markFailed(revision)
		}
		return err
	}
	if !priorSwitchAttempt {
		if err := m.registry.MarkSwitchStarted(ctx, revision.ServiceID, revision.Number); err != nil {
			if started {
				m.stopWorker(revision.ServiceID, revision.Number)
			}
			return err
		}
		revision.SwitchStarted = true
	}
	if err := m.setCurrent(ctx, revision, priorSwitchAttempt); err != nil {
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

func (m *Manager) setCurrent(ctx context.Context, revision registry.Revision, priorSwitchAttempt bool) error {
	build := buildID(revision.Number)
	var lastErr error
	unknownOutcome := priorSwitchAttempt
	for {
		lastErr = m.runtime.SetCurrent(ctx, revision.ServiceID, build)
		if lastErr == nil {
			return nil
		}
		definitiveRejection := definitiveSwitchRejection(lastErr)
		if !definitiveRejection {
			unknownOutcome = true
		}
		if !retryableSwitchError(lastErr) {
			return m.observeSwitchResult(revision, lastErr, nil, definitiveRejection && !unknownOutcome)
		}
		select {
		case <-ctx.Done():
			return m.observeSwitchResult(revision, lastErr, ctx.Err(), definitiveRejection && !unknownOutcome)
		case <-time.After(m.options.RetryInterval):
		}
	}
}

func (m *Manager) observeSwitchResult(revision registry.Revision, switchErr, contextErr error, knownIfDifferent bool) error {
	probeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	current, probeErr := m.runtime.Current(probeCtx, revision.ServiceID)
	cancel()
	if probeErr == nil && current == buildID(revision.Number) {
		return nil
	}
	return &switchError{
		err:   fmt.Errorf("set current version: %w", errors.Join(switchErr, contextErr, probeErr)),
		known: probeErr == nil && knownIfDifferent,
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
	code := switchErrorCode(err)
	switch code {
	case codes.NotFound, codes.FailedPrecondition, codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}

func definitiveSwitchRejection(err error) bool {
	switch switchErrorCode(err) {
	case codes.NotFound, codes.FailedPrecondition, codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated:
		return true
	default:
		return false
	}
}

func switchErrorCode(err error) codes.Code {
	code := status.Code(err)
	var temporalStatus interface{ Status() *status.Status }
	if errors.As(err, &temporalStatus) {
		code = temporalStatus.Status().Code()
	}
	return code
}

func (m *Manager) activityFor(revision registry.Revision) (Activity, error) {
	if revision.Kind == registry.KindTombstone {
		return tombstoneActivity, nil
	}
	var spec temporaless.ServiceSpec
	if err := json.Unmarshal(revision.SpecJSON, &spec); err != nil {
		return nil, fmt.Errorf("decode stored spec: %w", err)
	}
	compiled, err := backend.Compile(spec, m.client, m.options.MaxBackendResponseBodyBytes)
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

func (m *Manager) reconcileOperationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, m.options.SwitchTimeout)
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
