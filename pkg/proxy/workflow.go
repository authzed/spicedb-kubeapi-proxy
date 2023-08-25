package proxy

//
// import (
// 	"context"
// 	"fmt"
// 	"time"
//
// 	"go.temporal.io/sdk/activity"
// 	"go.temporal.io/sdk/interceptor"
// 	"go.temporal.io/sdk/worker"
// 	"go.temporal.io/sdk/workflow"
// )
//
// // Greet implements a Temporal workflow that returns a salutation for a given subject.
// func Greet(ctx workflow.Context, subject string) (string, error) {
// 	var greeting string
// 	if err := workflow.ExecuteActivity(
// 		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{ScheduleToCloseTimeout: time.Second}),
// 		PickGreeting,
// 	).Get(ctx, &greeting); err != nil {
// 		return "", err
// 	}
//
// 	return fmt.Sprintf("%s %s", greeting, subject), nil
// }
//
// // PickGreeting is a Temporal activity that returns some greeting text.
// func PickGreeting(ctx context.Context) (string, error) {
// 	return "Hello", nil
// }
//
// func HandleIntercept(ctx context.Context) (string, error) {
// 	return "Ok", nil
// }
//
// func RegisterWorkflowsAndActivities(r worker.Registry) {
// 	r.RegisterWorkflow(Greet)
// 	r.RegisterActivity(PickGreeting)
// 	r.RegisterActivityWithOptions(HandleIntercept, activity.RegisterOptions{Name: "HandleIntercept"})
// }
//
// // Example interceptor
//
// var _ interceptor.Interceptor = &Interceptor{}
//
// type Interceptor struct {
// 	interceptor.InterceptorBase
// }
//
// type WorkflowInterceptor struct {
// 	interceptor.WorkflowInboundInterceptorBase
// }
//
// func NewTestInterceptor() *Interceptor {
// 	return &Interceptor{}
// }
//
// func (i *Interceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
// 	return i.InterceptorBase.InterceptClient(next)
// }
//
// func (i *Interceptor) InterceptWorkflow(ctx workflow.Context, next interceptor.WorkflowInboundInterceptor) interceptor.WorkflowInboundInterceptor {
// 	return &WorkflowInterceptor{
// 		WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{
// 			Next: next,
// 		},
// 	}
// }
//
// func (i *WorkflowInterceptor) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
// 	return i.Next.Init(outbound)
// }
//
// func (i *WorkflowInterceptor) ExecuteWorkflow(ctx workflow.Context, in *interceptor.ExecuteWorkflowInput) (interface{}, error) {
// 	version := workflow.GetVersion(ctx, "version", workflow.DefaultVersion, 1)
// 	var err error
//
// 	if version != workflow.DefaultVersion {
// 		var vpt string
// 		err = workflow.ExecuteLocalActivity(
// 			workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{ScheduleToCloseTimeout: time.Second}),
// 			"HandleIntercept",
// 		).Get(ctx, &vpt)
//
// 		if err != nil {
// 			return nil, err
// 		}
// 	}
//
// 	return i.Next.ExecuteWorkflow(ctx, in)
// }
