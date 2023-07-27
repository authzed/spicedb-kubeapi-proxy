package proxy

import (
	"k8s.io/apimachinery/pkg/util/runtime"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	logsapi "k8s.io/component-base/logs/api/v1"
)

// TODO: can we get some of these from the upstream cluster?

var DefaultFeatureGate = utilfeature.DefaultFeatureGate

func init() {
	runtime.Must(utilfeature.DefaultMutableFeatureGate.Add(defaultGenericControlPlaneFeatureGates))
}

var defaultGenericControlPlaneFeatureGates = map[featuregate.Feature]featuregate.FeatureSpec{
	genericfeatures.AdvancedAuditing:                    {Default: true, PreRelease: featuregate.GA},
	genericfeatures.APIResponseCompression:              {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.APIListChunking:                     {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.APIPriorityAndFairness:              {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.CustomResourceValidationExpressions: {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.DryRun:                              {Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // remove in 1.28
	genericfeatures.OpenAPIEnums:                        {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.OpenAPIV3:                           {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.ServerSideApply:                     {Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // remove in 1.29
	genericfeatures.ServerSideFieldValidation:           {Default: true, PreRelease: featuregate.Beta},
	genericfeatures.ValidatingAdmissionPolicy:           {Default: false, PreRelease: featuregate.Alpha},
	logsapi.LoggingBetaOptions:                          {Default: true, PreRelease: featuregate.Beta},
	logsapi.ContextualLogging:                           {Default: true, PreRelease: featuregate.Alpha},
}
