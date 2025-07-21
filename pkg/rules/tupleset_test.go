package rules

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
)

func TestTupleSetExpr_GenerateRelationships(t *testing.T) {
	tests := []struct {
		name       string
		tupleSet   string
		input      *ResolveInput
		want       []*ResolvedRel
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:     "deployment containers",
			tupleSet: `this.namespacedName.(nsName -> this.object.spec.template.spec.containers.map_each("deployment:" + nsName + "#has-container@container:" + this.name))`,
			input: createDeploymentInput("test-deployment", "default", []containerSpec{
				{name: "server"},
				{name: "config-reloader"},
				{name: "proxy-sidecar"},
			}),
			want: []*ResolvedRel{
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "server",
				},
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "config-reloader",
				},
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "proxy-sidecar",
				},
			},
		},
		{
			name:     "empty container list",
			tupleSet: `this.namespacedName.(nsName -> this.object.spec.template.spec.containers.map_each("deployment:" + nsName + "#has-container@container:" + this.name))`,
			input:    createDeploymentInput("test-deployment", "default", []containerSpec{}),
			want:     []*ResolvedRel{},
		},
		{
			name:     "with filtering",
			tupleSet: `this.namespacedName.(nsName -> this.object.spec.template.spec.containers.filter(this.name != "proxy-sidecar").map_each("deployment:" + nsName + "#has-container@container:" + this.name))`,
			input: createDeploymentInput("test-deployment", "default", []containerSpec{
				{name: "server"},
				{name: "proxy-sidecar"},
			}),
			want: []*ResolvedRel{
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "server",
				},
			},
		},
		{
			name:     "service ports",
			tupleSet: `this.namespacedName.(nsName -> this.object.spec.ports.map_each("service:" + nsName + "#exposes-port@port:" + if this.name != null { this.name } else { this.port.string() }))`,
			input:    createServiceInput("test-service", "default"),
			want: []*ResolvedRel{
				{
					ResourceType:     "service",
					ResourceID:       "default/test-service",
					ResourceRelation: "exposes-port",
					SubjectType:      "port",
					SubjectID:        "http",
				},
				{
					ResourceType:     "service",
					ResourceID:       "default/test-service",
					ResourceRelation: "exposes-port",
					SubjectType:      "port",
					SubjectID:        "8080",
				},
			},
		},
		{
			name:       "non-array result",
			tupleSet:   `"single-string"`,
			input:      createSimpleInput("test", "default"),
			wantErr:    true,
			wantErrMsg: "tuple set expression must return an array",
		},
		{
			name:       "invalid relationship string",
			tupleSet:   `["invalid-relationship-format"]`,
			input:      createSimpleInput("test", "default"),
			wantErr:    true,
			wantErrMsg: "error parsing relationship string",
		},
		{
			name:     "handle null/missing fields safely",
			tupleSet: `this.namespacedName.(nsName -> (this.object.spec.template.spec.initContainers | []).map_each("deployment:" + nsName + "#has-init-container@container:" + this.name))`,
			input:    createDeploymentInput("test-deployment", "default", []containerSpec{{name: "server"}}), // no initContainers
			want:     []*ResolvedRel{},
		},
		{
			name: "let syntax with explicit root assignment",
			tupleSet: `let nsName = this.namespacedName
				root = this.object.spec.template.spec.containers.map_each("deployment:" + $nsName + "#has-container@container:" + this.name)`,
			input: createDeploymentInput("test-deployment", "default", []containerSpec{
				{name: "server"},
				{name: "sidecar"},
			}),
			want: []*ResolvedRel{
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "server",
				},
				{
					ResourceType:     "deployment",
					ResourceID:       "default/test-deployment",
					ResourceRelation: "has-container",
					SubjectType:      "container",
					SubjectID:        "sidecar",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor, err := CompileTupleSetExpression(tt.tupleSet)
			require.NoError(t, err)

			tupleSetExpr := &TupleSetExpr{
				Expression: executor,
			}

			got, err := tupleSetExpr.GenerateRelationships(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrMsg != "" {
					require.Contains(t, err.Error(), tt.wantErrMsg)
				}
				return
			}

			require.NoError(t, err)
			if tt.want == nil && got == nil {
				return // both nil is fine
			}
			if len(tt.want) == 0 && len(got) == 0 {
				return // both empty is fine
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCompileStringOrObjTemplatesWithTupleSet(t *testing.T) {
	templates := []proxyrule.StringOrTemplate{
		{
			Template: "deployment:{{namespacedName}}#creator@user:{{user.name}}",
		},
		{
			TupleSet: `this.namespacedName.(nsName -> this.object.spec.template.spec.containers.map_each("deployment:" + nsName + "#has-container@container:" + this.name))`,
		},
	}

	exprs, err := compileStringOrObjTemplates(templates)
	require.NoError(t, err)
	require.Len(t, exprs, 2)

	// First should be a RelExpr
	_, ok := exprs[0].(*RelExpr)
	require.True(t, ok, "First expression should be a RelExpr")

	// Second should be a TupleSetExpr
	_, ok = exprs[1].(*TupleSetExpr)
	require.True(t, ok, "Second expression should be a TupleSetExpr")
}

func TestTupleSetValidation(t *testing.T) {
	tests := []struct {
		name     string
		template proxyrule.StringOrTemplate
		wantErr  bool
	}{
		{
			name: "valid tupleSet only",
			template: proxyrule.StringOrTemplate{
				TupleSet: `["test"]`,
			},
			wantErr: false,
		},
		{
			name: "valid template only",
			template: proxyrule.StringOrTemplate{
				Template: "test:{{name}}#rel@user:{{user.name}}",
			},
			wantErr: false,
		},
		{
			name: "tupleSet with template - should fail validation at config level",
			template: proxyrule.StringOrTemplate{
				Template: "test:{{name}}#rel@user:{{user.name}}",
				TupleSet: `["test"]`,
			},
			// This should be caught by config validation, not compilation
			wantErr: false, // compilation doesn't validate mutual exclusion
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := compileStringOrObjTemplates([]proxyrule.StringOrTemplate{tt.template})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Test helpers

type containerSpec struct {
	name  string
	image string
}

func createDeploymentInput(name, namespace string, containers []containerSpec) *ResolveInput {
	containerObjs := make([]map[string]any, len(containers))
	for i, c := range containers {
		containerObjs[i] = map[string]any{
			"name": c.name,
		}
		if c.image != "" {
			containerObjs[i]["image"] = c.image
		}
	}

	deploymentSpec := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": containerObjs,
				},
			},
		},
	}

	body, _ := json.Marshal(deploymentSpec)

	return &ResolveInput{
		Name:           name,
		Namespace:      namespace,
		NamespacedName: namespace + "/" + name,
		Request: &request.RequestInfo{
			Verb:     "create",
			Resource: "deployments",
		},
		User: &user.DefaultInfo{
			Name: "testuser",
		},
		Object: &metav1.PartialObjectMetadata{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		},
		Body: body,
	}
}

func createServiceInput(name, namespace string) *ResolveInput {
	serviceSpec := map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"spec": map[string]any{
			"ports": []map[string]any{
				{
					"name": "http",
					"port": 80,
				},
				{
					"port": 8080,
				},
			},
		},
	}

	body, _ := json.Marshal(serviceSpec)

	return &ResolveInput{
		Name:           name,
		Namespace:      namespace,
		NamespacedName: namespace + "/" + name,
		Request: &request.RequestInfo{
			Verb:     "create",
			Resource: "services",
		},
		User: &user.DefaultInfo{
			Name: "testuser",
		},
		Object: &metav1.PartialObjectMetadata{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		},
		Body: body,
	}
}

func createSimpleInput(name, namespace string) *ResolveInput {
	return &ResolveInput{
		Name:           name,
		Namespace:      namespace,
		NamespacedName: namespace + "/" + name,
		Request: &request.RequestInfo{
			Verb:     "create",
			Resource: "pods",
		},
		User: &user.DefaultInfo{
			Name: "testuser",
		},
	}
}

func TestTupleSetInChecksAndPreconditions(t *testing.T) {
	tests := []struct {
		name     string
		tupleSet string
		input    *ResolveInput
		want     []*ResolvedRel
		wantErr  bool
	}{
		{
			name:     "check all containers in deployment",
			tupleSet: `this.user.name.(userName -> this.object.spec.template.spec.containers.map_each("container:" + this.name + "#deploy@user:" + userName))`,
			input: createDeploymentInput("web-app", "production", []containerSpec{
				{name: "server"},
				{name: "sidecar"},
			}),
			want: []*ResolvedRel{
				{
					ResourceType:     "container",
					ResourceID:       "server",
					ResourceRelation: "deploy",
					SubjectType:      "user",
					SubjectID:        "testuser",
				},
				{
					ResourceType:     "container",
					ResourceID:       "sidecar",
					ResourceRelation: "deploy",
					SubjectType:      "user",
					SubjectID:        "testuser",
				},
			},
		},
		{
			name:     "precondition for multiple secrets",
			tupleSet: `["secret:" + this.namespace + "/secret1#exists@system:cluster", "secret:" + this.namespace + "/secret2#exists@system:cluster"]`,
			input:    createSimpleInput("test-pod", "default"),
			want: []*ResolvedRel{
				{
					ResourceType:     "secret",
					ResourceID:       "default/secret1",
					ResourceRelation: "exists",
					SubjectType:      "system",
					SubjectID:        "cluster",
				},
				{
					ResourceType:     "secret",
					ResourceID:       "default/secret2",
					ResourceRelation: "exists",
					SubjectType:      "system",
					SubjectID:        "cluster",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor, err := CompileTupleSetExpression(tt.tupleSet)
			require.NoError(t, err)

			tupleSetExpr := &TupleSetExpr{
				Expression: executor,
			}

			got, err := tupleSetExpr.GenerateRelationships(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.want == nil && got == nil {
				return // both nil is fine
			}
			if len(tt.want) == 0 && len(got) == 0 {
				return // both empty is fine
			}
			require.Equal(t, tt.want, got)
		})
	}
}
