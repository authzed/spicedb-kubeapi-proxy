package proxy

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	logsapi "k8s.io/component-base/logs/api/v1"
)

func init() {
	runtime.Must(utilfeature.DefaultMutableFeatureGate.Add(defaultGenericControlPlaneFeatureGates))
}

var defaultGenericControlPlaneFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
	genericfeatures.APIResponseCompression:              {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.APIListChunking:                     {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.APIPriorityAndFairness:              {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.CustomResourceValidationExpressions: {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.OpenAPIEnums:                        {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.ServerSideApply:                     {Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // remove in 1.29
	genericfeatures.ValidatingAdmissionPolicy:           {Default: false, PreRelease: featuregate.Beta},
	logsapi.LoggingBetaOptions:                          {Default: true, PreRelease: featuregate.Beta},
	logsapi.ContextualLogging:                           {Default: true, PreRelease: featuregate.Alpha},
}
