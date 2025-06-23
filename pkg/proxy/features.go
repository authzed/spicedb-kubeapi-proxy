package proxy

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	logsapi "k8s.io/component-base/logs/api/v1"
)

func init() {
	runtime.Must(utilfeature.DefaultMutableFeatureGate.Add(defaultGenericControlPlaneFeatureGates))
}

var defaultGenericControlPlaneFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
	// Keep only the logging features that are required for the logging system to work
	logsapi.LoggingBetaOptions: {Default: true, PreRelease: featuregate.Beta},
	logsapi.ContextualLogging:  {Default: true, PreRelease: featuregate.Alpha},
	
	// Note: The following features have been removed from apiserver in Kubernetes 1.33:
	// - APIListChunking: Promoted to GA and always enabled
	// - APIPriorityAndFairness: Promoted to GA and always enabled  
	// - CustomResourceValidationExpressions: Moved to apiextensions-apiserver
	// - ServerSideApply: Promoted to GA and always enabled
	// - ValidatingAdmissionPolicy: Moved to a different package or promoted to GA
	// - APIResponseCompression: Already registered in default feature gates
	// - OpenAPIEnums: Already registered in default feature gates
}
