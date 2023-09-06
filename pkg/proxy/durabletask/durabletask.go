package durabletask

import (
	"context"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy/distributedtx"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
	"github.com/microsoft/durabletask-go/task"
	"k8s.io/client-go/rest"
)

const MemorySQLite = ""

type TaskWorker interface {
	Start(context.Context) error
	Shutdown(context.Context) error
}

func Setup(permissionClient v1.PermissionsServiceClient, kubeClient rest.Interface, sqlitePath string) (distributedtx.TaskScheduler, TaskWorker, error) {
	h := TaskHandler{distributedtx.Handler{PermissionClient: permissionClient, KubeClient: kubeClient}}

	// durabletask - stores planned dual writes in a sqlite db
	logger := backend.DefaultLogger()
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN(distributedtx.StrategyPessimisticWriteToSpiceDBAndKube, PessimisticWriteToSpiceDBAndKube); err != nil {
		return nil, nil, err
	}
	if err := r.AddOrchestratorN(distributedtx.StrategyOptimisticWriteToSpiceDBAndKube, OptimisticWriteToSpiceDBAndKube); err != nil {
		return nil, nil, err
	}
	if err := r.AddActivityN(distributedtx.WriteToSpiceDB, h.WriteToSpiceDB); err != nil {
		return nil, nil, err
	}
	if err := r.AddActivityN(distributedtx.WriteToKube, h.WriteToKube); err != nil {
		return nil, nil, err
	}
	if err := r.AddActivityN(distributedtx.CheckKube, h.CheckKube); err != nil {
		return nil, nil, err
	}

	// note: can use the in-memory sqlite provider by specifying ""
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(sqlitePath), logger)
	executor := task.NewTaskExecutor(r)
	orchestrationWorker := backend.NewOrchestrationWorker(be, executor, logger)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	worker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)

	taskScheduler := TaskScheduler{
		taskHubClient: backend.NewTaskHubClient(be),
	}
	return taskScheduler, worker, nil
}

func PessimisticWriteToSpiceDBAndKube(ctx *task.OrchestrationContext) (any, error) {
	ec := ExecutionContext{ctx: ctx}
	return distributedtx.PessimisticWriteToSpiceDBAndKube(ec, ec)
}

func OptimisticWriteToSpiceDBAndKube(ctx *task.OrchestrationContext) (any, error) {
	ec := ExecutionContext{ctx: ctx}
	return distributedtx.OptimisticWriteToSpiceDBAndKube(ec, ec)
}

type ExecutionContext struct {
	ctx *task.OrchestrationContext
}

func (e ExecutionContext) Call(name string, arg any) distributedtx.Task {
	return e.ctx.CallActivity(name, task.WithActivityInput(arg))
}

func (e ExecutionContext) GetInput(resultPtr any) error {
	return e.ctx.GetInput(resultPtr)
}

func (e ExecutionContext) ID() string {
	return string(e.ctx.ID)
}

type TaskScheduler struct {
	taskHubClient backend.TaskHubClient
}

func (t TaskScheduler) Schedule(ctx context.Context, orchestrator string, opts ...any) (string, error) {
	var orchestrationOpts []api.NewOrchestrationOptions
	for _, opt := range opts {
		orchestrationOpts = append(orchestrationOpts, api.WithInput(opt))
	}

	instanceID, err := t.taskHubClient.ScheduleNewOrchestration(ctx, orchestrator, orchestrationOpts...)
	if err != nil {
		return string(instanceID), err
	}

	return string(instanceID), nil
}

func (t TaskScheduler) WaitForCompletion(ctx context.Context, id string) (*distributedtx.OrchestrationResult, error) {
	metadata, err := t.taskHubClient.WaitForOrchestrationCompletion(ctx, api.InstanceID(id))
	if err != nil {
		return nil, err
	}
	var errMsg string
	if metadata.FailureDetails != nil {
		errMsg = metadata.FailureDetails.ErrorMessage
	}
	return &distributedtx.OrchestrationResult{
		SerializedOutput: []byte(metadata.SerializedOutput),
		Err:              errMsg,
	}, nil
}

type TaskHandler struct {
	distributedtx.Handler
}

func (th TaskHandler) WriteToSpiceDB(ctx task.ActivityContext) (any, error) {
	return th.Handler.WriteToSpiceDB(ctx)
}

func (th TaskHandler) WriteToKube(ctx task.ActivityContext) (any, error) {
	return th.Handler.WriteToKube(ctx)
}

func (th TaskHandler) CheckKube(ctx task.ActivityContext) (any, error) {
	return th.Handler.CheckKube(ctx)
}
