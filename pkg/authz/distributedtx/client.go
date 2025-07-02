package distributedtx

import (
	"context"
	"log/slog"
	"os"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/monoprocess"
	"github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

const defaultLogLevel = slog.LevelDebug

func SetupWithMemoryBackend(ctx context.Context, permissionClient v1.PermissionsServiceClient, kubeClient rest.Interface) (*client.Client, *Worker, error) {
	ctx = klog.NewContext(ctx, klog.FromContext(ctx).WithValues("backend", "sqlite-memory"))
	return SetupWithBackend(ctx, permissionClient, kubeClient, sqlite.NewInMemoryBackend(backend.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: defaultLogLevel,
	})))))
}

func SetupWithSQLiteBackend(ctx context.Context, permissionClient v1.PermissionsServiceClient, kubeClient rest.Interface, sqlitePath string) (*client.Client, *Worker, error) {
	if sqlitePath == "" {
		return SetupWithMemoryBackend(ctx, permissionClient, kubeClient)
	}

	ctx = klog.NewContext(ctx, klog.FromContext(ctx).WithValues("backend", "sqlite-file", "path", sqlitePath))
	return SetupWithBackend(ctx, permissionClient, kubeClient, sqlite.NewSqliteBackend(sqlitePath, backend.WithLogger(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: defaultLogLevel,
	})))))
}

func SetupWithBackend(ctx context.Context, permissionClient v1.PermissionsServiceClient, kubeClient rest.Interface, backend backend.Backend) (*client.Client, *Worker, error) {
	klog.FromContext(ctx).Info("starting workflow engine")
	txHandler := ActivityHandler{
		PermissionClient: permissionClient,
		KubeClient:       kubeClient,
	}

	monoBackend := monoprocess.NewMonoprocessBackend(backend)
	w := worker.New(monoBackend, &worker.DefaultWorkerOptions)

	if err := w.RegisterWorkflow(PessimisticWriteToSpiceDBAndKube); err != nil {
		return nil, nil, err
	}
	if err := w.RegisterWorkflow(OptimisticWriteToSpiceDBAndKube); err != nil {
		return nil, nil, err
	}
	if err := w.RegisterActivity(txHandler.WriteToKube); err != nil {
		return nil, nil, err
	}
	if err := w.RegisterActivity(txHandler.CheckKubeResource); err != nil {
		return nil, nil, err
	}
	if err := w.RegisterActivity(txHandler.WriteToSpiceDB); err != nil {
		return nil, nil, err
	}
	if err := w.RegisterActivity(txHandler.ReadRelationships); err != nil {
		return nil, nil, err
	}

	return client.New(monoBackend), &Worker{worker: w}, nil
}

type Worker struct {
	worker       *worker.Worker
	shutdownFunc func()
}

func (w *Worker) Start(ctx context.Context) error {
	ctx, w.shutdownFunc = context.WithCancel(ctx)
	return w.worker.Start(ctx)
}

func (w *Worker) Shutdown(_ context.Context) error {
	w.shutdownFunc()
	return w.worker.WaitForCompletion()
}
