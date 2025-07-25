//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/pointer"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/authz/distributedtx"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/failpoints"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

var _ = Describe("Proxy", func() {
	When("there are two users", func() {
		var paulClient, chaniClient, adminClient kubernetes.Interface
		var paulDynamicClient, chaniDynamicClient, adminDynamicClient dynamic.Interface
		var paulRestConfig, chaniRestConfig, adminRestConfig *rest.Config
		var paulNamespace, chaniNamespace, sharedNamespace string
		var chaniPod, paulPod string
		var paulCustomResource, chaniCustomResource string
		var lockMode proxyrule.LockMode

		BeforeEach(func() {
			var err error
			paulRestConfig, err = clientcmd.NewDefaultClientConfig(
				*clientCA.GenerateUserConfig("paul"), nil,
			).ClientConfig()
			Expect(err).To(Succeed())
			paulClient, err = kubernetes.NewForConfig(paulRestConfig)
			Expect(err).To(Succeed())
			paulDynamicClient, err = dynamic.NewForConfig(paulRestConfig)
			Expect(err).To(Succeed())

			chaniRestConfig, err = clientcmd.NewDefaultClientConfig(
				*clientCA.GenerateUserConfig("chani"), nil,
			).ClientConfig()
			Expect(err).To(Succeed())
			chaniClient, err = kubernetes.NewForConfig(chaniRestConfig)
			Expect(err).To(Succeed())
			chaniDynamicClient, err = dynamic.NewForConfig(chaniRestConfig)
			Expect(err).To(Succeed())

			adminClient, err = kubernetes.NewForConfig(adminUser.Config())
			Expect(err).To(Succeed())
			adminRestConfig = adminUser.Config()
			adminDynamicClient, err = dynamic.NewForConfig(adminRestConfig)
			Expect(err).To(Succeed())

			paulNamespace = names.SimpleNameGenerator.GenerateName("paul-")
			chaniNamespace = names.SimpleNameGenerator.GenerateName("chani-")

			// pods are used for tests that require deletes, since the GC
			// controller can clean them up in tests (namespaces can't be GCd)
			sharedNamespace = names.SimpleNameGenerator.GenerateName("shared-")
			paulPod = names.SimpleNameGenerator.GenerateName("paul-pod-")
			chaniPod = names.SimpleNameGenerator.GenerateName("chani-pod-")
			paulCustomResource = names.SimpleNameGenerator.GenerateName("paul-cr-")
			chaniCustomResource = names.SimpleNameGenerator.GenerateName("chani-cr-")
		})

		AfterEach(func(ctx context.Context) {
			orphan := metav1.DeletePropagationOrphan
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, paulNamespace, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, chaniNamespace, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminClient.CoreV1().Pods(sharedNamespace).Delete(ctx, paulPod, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminClient.CoreV1().Pods(sharedNamespace).Delete(ctx, chaniPod, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, sharedNamespace, metav1.DeleteOptions{PropagationPolicy: &orphan})

			// Clean up custom resources
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
			_ = adminDynamicClient.Resource(gvr).Namespace(paulNamespace).Delete(ctx, paulCustomResource, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminDynamicClient.Resource(gvr).Namespace(chaniNamespace).Delete(ctx, chaniCustomResource, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminDynamicClient.Resource(gvr).Namespace(sharedNamespace).Delete(ctx, paulCustomResource, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminDynamicClient.Resource(gvr).Namespace(sharedNamespace).Delete(ctx, chaniCustomResource, metav1.DeleteOptions{PropagationPolicy: &orphan})

			gvr = schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "anothertestresources"}
			_ = adminDynamicClient.Resource(gvr).Namespace(paulNamespace).Delete(ctx, paulCustomResource, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminDynamicClient.Resource(gvr).Namespace(chaniNamespace).Delete(ctx, chaniCustomResource, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminDynamicClient.Resource(gvr).Namespace(sharedNamespace).Delete(ctx, paulCustomResource, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminDynamicClient.Resource(gvr).Namespace(sharedNamespace).Delete(ctx, chaniCustomResource, metav1.DeleteOptions{PropagationPolicy: &orphan})

			// ensure there are no remaining locks
			Expect(len(GetAllTuples(ctx, &v1.RelationshipFilter{
				ResourceType:          "lock",
				OptionalRelation:      "workflow",
				OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "workflow"},
			}))).To(BeZero())

			// prevent failpoints from bleeding between tests
			failpoints.DisableAll()
		})

		CreateNamespace := func(ctx context.Context, client kubernetes.Interface, namespace string) error {
			_, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: namespace},
			}, metav1.CreateOptions{})
			return err
		}
		GetNamespace := func(ctx context.Context, client kubernetes.Interface, namespace string) error {
			_, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
			return err
		}
		ListNamespaces := func(ctx context.Context, client kubernetes.Interface) []string {
			visibleNamespaces, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			return lo.Map(visibleNamespaces.Items, func(item corev1.Namespace, index int) string {
				return item.Name
			})
		}
		WatchNamespaces := func(ctx context.Context, client kubernetes.Interface, expected int) []string {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			got := make([]string, 0, expected)
			watcher, err := client.CoreV1().Namespaces().Watch(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			defer watcher.Stop()

			for e := range watcher.ResultChan() {
				ns, ok := e.Object.(*corev1.Namespace)
				if !ok {
					return got
				}
				got = append(got, ns.Name)
				if len(got) == expected {
					return got
				}
			}
			return got
		}

		CreatePod := func(ctx context.Context, client kubernetes.Interface, namespace, name string) error {
			_, err := client.CoreV1().Pods(namespace).Create(ctx, &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name:  "nginx",
					Image: "nginx:1.14.2",
				}}},
			}, metav1.CreateOptions{})
			return err
		}
		DeletePod := func(ctx context.Context, client kubernetes.Interface, namespace, name string) error {
			return client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
		}
		GetPod := func(ctx context.Context, client kubernetes.Interface, namespace, name string) error {
			_, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
			return err
		}
		GetAndUpdatePodLabels := func(ctx context.Context, client kubernetes.Interface, namespace, name string, labels map[string]string) error {
			pod, err := client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			pod.Labels = labels
			_, err = client.CoreV1().Pods(namespace).Update(ctx, pod, metav1.UpdateOptions{})
			return err
		}
		PatchPodLabels := func(ctx context.Context, client kubernetes.Interface, namespace, name string, labels map[string]string) error {
			patch := &corev1.Pod{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Pod",
					APIVersion: "v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
					Labels:    labels,
				},
			}
			data, err := json.Marshal(patch)
			if err != nil {
				return err
			}
			_, err = client.CoreV1().Pods(namespace).Patch(ctx, name, types.ApplyPatchType, data, metav1.PatchOptions{FieldManager: "Test", Force: pointer.Bool(true)})
			return err
		}
		ListPods := func(ctx context.Context, client kubernetes.Interface, namespace string) []string {
			visibleNamespaces, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			return lo.Map(visibleNamespaces.Items, func(item corev1.Pod, index int) string {
				return item.Name
			})
		}
		ListPodsAsTable := func(ctx context.Context, config *rest.Config, namespace string) (*metav1.Table, error) {
			cfg := rest.CopyConfig(config)
			gv := corev1.SchemeGroupVersion
			cfg.GroupVersion = &gv
			cfg.APIPath = "/api"
			cfg.NegotiatedSerializer = rest.CodecFactoryForGeneratedClient(scheme.Scheme, scheme.Codecs).WithoutConversion()
			if cfg.UserAgent == "" {
				cfg.UserAgent = rest.DefaultKubernetesUserAgent()
			}

			client, err := rest.RESTClientFor(cfg)
			Expect(err).To(Succeed())
			req := client.Get().
				Resource("pods").
				Namespace(namespace).
				SetHeader("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io")

			result := req.Do(ctx)
			if result.Error() != nil {
				return nil, result.Error()
			}

			body, err := result.Raw()
			if err != nil {
				return nil, err
			}

			var table metav1.Table
			err = json.Unmarshal(body, &table)
			if err != nil {
				return nil, err
			}

			return &table, nil
		}
		GetPodAsTable := func(ctx context.Context, config *rest.Config, namespace, name string) (*metav1.Table, error) {
			cfg := rest.CopyConfig(config)
			gv := corev1.SchemeGroupVersion
			cfg.GroupVersion = &gv
			cfg.APIPath = "/api"
			cfg.NegotiatedSerializer = rest.CodecFactoryForGeneratedClient(scheme.Scheme, scheme.Codecs).WithoutConversion()
			if cfg.UserAgent == "" {
				cfg.UserAgent = rest.DefaultKubernetesUserAgent()
			}

			client, err := rest.RESTClientFor(cfg)
			Expect(err).To(Succeed())
			req := client.Get().
				Resource("pods").
				Namespace(namespace).
				Name(name).
				SetHeader("Accept", "application/json;as=Table;v=v1;g=meta.k8s.io")

			result := req.Do(ctx)
			if result.Error() != nil {
				return nil, result.Error()
			}

			body, err := result.Raw()
			if err != nil {
				return nil, err
			}

			var table metav1.Table
			err = json.Unmarshal(body, &table)
			if err != nil {
				return nil, err
			}

			return &table, nil
		}
		WatchPods := func(ctx context.Context, client kubernetes.Interface, namespace string, expected int, timeout time.Duration) []string {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			got := make([]string, 0, expected)
			watcher, err := client.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
				ResourceVersion: "0",
			})
			Expect(err).To(Succeed())
			defer watcher.Stop()

			for e := range watcher.ResultChan() {
				pod, ok := e.Object.(*corev1.Pod)
				if !ok {
					return got
				}
				got = append(got, pod.Name)
				if len(got) == expected {
					return got
				}
			}
			return got
		}

		// Custom Resource helper functions
		CreateTestResource := func(ctx context.Context, client dynamic.Interface, namespace, name string) error {
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
			resource := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "example.com/v1",
					"kind":       "TestResource",
					"metadata": map[string]interface{}{
						"name":      name,
						"namespace": namespace,
					},
					"spec": map[string]interface{}{
						"message": "test message",
					},
				},
			}
			_, err := client.Resource(gvr).Namespace(namespace).Create(ctx, resource, metav1.CreateOptions{})
			return err
		}

		DeleteTestResource := func(ctx context.Context, client dynamic.Interface, namespace, name string) error {
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
			return client.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
		}

		GetTestResource := func(ctx context.Context, client dynamic.Interface, namespace, name string) error {
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
			_, err := client.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
			return err
		}

		UpdateTestResource := func(ctx context.Context, client dynamic.Interface, namespace, name string, message string) error {
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
			resource, err := client.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			spec, ok := resource.Object["spec"].(map[string]interface{})
			if !ok {
				spec = make(map[string]interface{})
				resource.Object["spec"] = spec
			}
			spec["message"] = message
			_, err = client.Resource(gvr).Namespace(namespace).Update(ctx, resource, metav1.UpdateOptions{})
			return err
		}

		ListTestResources := func(ctx context.Context, client dynamic.Interface, namespace string) []string {
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
			list, err := client.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			return lo.Map(list.Items, func(item unstructured.Unstructured, index int) string {
				return item.GetName()
			})
		}

		CreateAnotherTestResource := func(ctx context.Context, client dynamic.Interface, namespace, name string) error {
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "anothertestresources"}
			resource := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "example.com/v1",
					"kind":       "AnotherTestResource",
					"metadata": map[string]interface{}{
						"name":      name,
						"namespace": namespace,
					},
					"spec": map[string]interface{}{
						"message": "another test message",
					},
				},
			}
			_, err := client.Resource(gvr).Namespace(namespace).Create(ctx, resource, metav1.CreateOptions{})
			return err
		}

		ListAnotherTestResources := func(ctx context.Context, client dynamic.Interface, namespace string) []string {
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "anothertestresources"}
			list, err := client.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			return lo.Map(list.Items, func(item unstructured.Unstructured, index int) string {
				return item.GetName()
			})
		}

		DeleteAnotherTestResource := func(ctx context.Context, client dynamic.Interface, namespace, name string) error {
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "anothertestresources"}
			return client.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
		}

		GetAnotherTestResource := func(ctx context.Context, client dynamic.Interface, namespace, name string) error {
			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "anothertestresources"}
			_, err := client.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
			return err
		}

		WatchTestResources := func(ctx context.Context, client dynamic.Interface, namespace string, expected int, timeout time.Duration) []string {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			gvr := schema.GroupVersionResource{Group: "example.com", Version: "v1", Resource: "testresources"}
			got := make([]string, 0, expected)
			watcher, err := client.Resource(gvr).Namespace(namespace).Watch(ctx, metav1.ListOptions{
				ResourceVersion: "0",
			})
			Expect(err).To(Succeed())
			defer watcher.Stop()

			for e := range watcher.ResultChan() {
				fmt.Println("Event received:", e.Type, e.Object)
				resource, ok := e.Object.(*unstructured.Unstructured)
				if !ok {
					return got
				}
				got = append(got, resource.GetName())
				if len(got) == expected {
					return got
				}
			}
			return got
		}

		JustBeforeEach(func(ctx context.Context) {
			// before every test, assert no access
			Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, paulClient, paulNamespace))).To(BeTrue())
			Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, paulClient, chaniNamespace))).To(BeTrue())
			Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, chaniClient, paulNamespace))).To(BeTrue())
			Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())
		})

		AssertDualWriteBehavior := func() {

			It("supports rules for every verb", func(ctx context.Context) {
				// watch
				var wg errgroup.Group
				defer wg.Wait()
				wg.Go(func() error {
					defer GinkgoRecover()
					Expect(WatchPods(ctx, paulClient, paulNamespace, 1, 10*time.Second)).To(ContainElement(paulPod))
					return nil
				})

				// create
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreatePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())

				// get
				Expect(GetPod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())
				Expect(GetPod(ctx, chaniClient, paulNamespace, paulPod)).To(Not(Succeed()))

				// update
				Expect(GetAndUpdatePodLabels(ctx, paulClient, paulNamespace, paulPod, map[string]string{"a": "label"})).To(Succeed())
				Expect(GetAndUpdatePodLabels(ctx, chaniClient, paulNamespace, paulPod, map[string]string{"a": "label"})).To(Not(Succeed()))

				// patch
				Expect(PatchPodLabels(ctx, paulClient, paulNamespace, paulPod, map[string]string{"b": "label"})).To(Succeed())
				Expect(PatchPodLabels(ctx, chaniClient, paulNamespace, paulPod, map[string]string{"b": "label"})).To(Not(Succeed()))

				// list
				Expect(ListPods(ctx, paulClient, paulNamespace)).To(Equal([]string{paulPod}))
				Expect(ListPods(ctx, chaniClient, paulNamespace)).To(Equal([]string{}))

				// delete
				Expect(DeletePod(ctx, chaniClient, paulNamespace, paulPod)).To(Not(Succeed()))
				Expect(DeletePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())
			})

			It("supports rules for every verb on custom resources", func(ctx context.Context) {
				// watch
				var wg errgroup.Group
				defer wg.Wait()
				wg.Go(func() error {
					defer GinkgoRecover()
					Expect(WatchTestResources(ctx, paulDynamicClient, paulNamespace, 1, 10*time.Second)).To(ContainElement(paulCustomResource))
					return nil
				})

				// create
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreateTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Succeed())

				// get
				Expect(GetTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Succeed())
				Expect(GetTestResource(ctx, chaniDynamicClient, paulNamespace, paulCustomResource)).To(Not(Succeed()))

				// update
				Expect(UpdateTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource, "updated message")).To(Succeed())
				Expect(UpdateTestResource(ctx, chaniDynamicClient, paulNamespace, paulCustomResource, "unauthorized update")).To(Not(Succeed()))

				// list
				Expect(ListTestResources(ctx, paulDynamicClient, paulNamespace)).To(Equal([]string{paulCustomResource}))
				Expect(ListTestResources(ctx, chaniDynamicClient, paulNamespace)).To(Equal([]string{}))

				// delete
				Expect(DeleteTestResource(ctx, chaniDynamicClient, paulNamespace, paulCustomResource)).To(Not(Succeed()))
				Expect(DeleteTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Succeed())
			})

			It("supports postchecks and deletions by filters", func(ctx context.Context) {
				// create
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreateAnotherTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Succeed())

				// get
				Expect(GetAnotherTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Succeed())
				Expect(GetAnotherTestResource(ctx, chaniDynamicClient, paulNamespace, paulCustomResource)).To(Not(Succeed()))

				// list
				Expect(ListAnotherTestResources(ctx, paulDynamicClient, paulNamespace)).To(Equal([]string{paulCustomResource}))

				// delete
				Expect(DeleteAnotherTestResource(ctx, chaniDynamicClient, paulNamespace, paulCustomResource)).To(Not(Succeed()))
				Expect(DeleteAnotherTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Succeed())

				Expect(GetAnotherTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Not(Succeed()))
			})

			It("recovers when there is a failure in reading relationships for deletions by filters", func(ctx context.Context) {
				// create
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreateAnotherTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Succeed())

				// get
				Expect(GetAnotherTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Succeed())
				Expect(GetAnotherTestResource(ctx, chaniDynamicClient, paulNamespace, paulCustomResource)).To(Not(Succeed()))

				// delete
				failpoints.EnableFailPoint("panicReadSpiceDB", 1)
				Expect(DeleteAnotherTestResource(ctx, chaniDynamicClient, paulNamespace, paulCustomResource)).To(Not(Succeed()))
				Expect(DeleteAnotherTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Succeed())

				Expect(GetAnotherTestResource(ctx, paulDynamicClient, paulNamespace, paulCustomResource)).To(Not(Succeed()))
			})

			It("filters table format responses for get and list operations", func(ctx context.Context) {
				// Create namespaces and pods for both users
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreateNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())
				Expect(CreatePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())
				Expect(CreatePod(ctx, chaniClient, chaniNamespace, chaniPod)).To(Succeed())

				// Paul should be able to get his pod as a table
				paulTable, err := GetPodAsTable(ctx, paulRestConfig, paulNamespace, paulPod)
				Expect(err).To(Succeed())
				Expect(paulTable.Kind).To(Equal("Table"))
				Expect(len(paulTable.Rows)).To(Equal(1))

				// Extract the pod name from the table row
				var podObj corev1.Pod
				err = json.Unmarshal(paulTable.Rows[0].Object.Raw, &podObj)
				Expect(err).To(Succeed())
				Expect(podObj.Name).To(Equal(paulPod))

				// Paul should not be able to get Chani's pod as a table (should get unauthorized)
				chaniTable, err := GetPodAsTable(ctx, paulRestConfig, chaniNamespace, chaniPod)
				Expect(k8serrors.IsUnauthorized(err)).To(BeTrue())
				Expect(chaniTable).To(BeNil())

				// Test LIST operations with table format
				// Create a shared namespace with pods from both users
				Expect(CreateNamespace(ctx, adminClient, sharedNamespace)).To(Succeed())
				Expect(CreatePod(ctx, paulClient, sharedNamespace, paulPod)).To(Succeed())
				Expect(CreatePod(ctx, chaniClient, sharedNamespace, chaniPod)).To(Succeed())

				// Paul should only see his pod in the table list
				paulListTable, err := ListPodsAsTable(ctx, paulRestConfig, sharedNamespace)
				Expect(err).To(Succeed())
				Expect(paulListTable.Kind).To(Equal("Table"))
				Expect(len(paulListTable.Rows)).To(Equal(1))

				var paulPodFromList corev1.Pod
				err = json.Unmarshal(paulListTable.Rows[0].Object.Raw, &paulPodFromList)
				Expect(err).To(Succeed())
				Expect(paulPodFromList.Name).To(Equal(paulPod))

				// Chani should only see her pod in the table list
				chaniListTable, err := ListPodsAsTable(ctx, chaniRestConfig, sharedNamespace)
				Expect(err).To(Succeed())
				Expect(chaniListTable.Kind).To(Equal("Table"))
				Expect(len(chaniListTable.Rows)).To(Equal(1))

				var chaniPodFromList corev1.Pod
				err = json.Unmarshal(chaniListTable.Rows[0].Object.Raw, &chaniPodFromList)
				Expect(err).To(Succeed())
				Expect(chaniPodFromList.Name).To(Equal(chaniPod))

				// Admin should see both pods in the table list
				adminListTable, err := ListPodsAsTable(ctx, adminRestConfig, sharedNamespace)
				Expect(err).To(Succeed())
				Expect(adminListTable.Kind).To(Equal("Table"))
				Expect(len(adminListTable.Rows)).To(Equal(2))

				// Extract pod names from admin's view
				adminPodNames := make([]string, 0, 2)
				for _, row := range adminListTable.Rows {
					var pod corev1.Pod
					err = json.Unmarshal(row.Object.Raw, &pod)
					Expect(err).To(Succeed())
					adminPodNames = append(adminPodNames, pod.Name)
				}
				Expect(adminPodNames).To(ContainElements(paulPod, chaniPod))
			})

			It("doesn't show users namespaces the other has created", func(ctx context.Context) {
				var wg errgroup.Group
				defer wg.Wait()
				wg.Go(func() error {
					defer GinkgoRecover()
					Expect(WatchNamespaces(ctx, paulClient, 1)).To(ContainElement(paulNamespace))
					return nil
				})
				wg.Go(func() error {
					defer GinkgoRecover()
					Expect(WatchNamespaces(ctx, chaniClient, 1)).To(ContainElement(chaniNamespace))
					return nil
				})

				// each creates their respective namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreateNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())

				// each can get their respective namespace
				Expect(GetNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(GetNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())

				// neither can get each other's namespace
				Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, paulClient, chaniNamespace))).To(BeTrue())
				Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, chaniClient, paulNamespace))).To(BeTrue())

				// neither can see each other's namespace in the list
				paulList := ListNamespaces(ctx, paulClient)
				chaniList := ListNamespaces(ctx, chaniClient)
				Expect(paulList).ToNot(ContainElement(chaniNamespace))
				Expect(paulList).To(ContainElement(paulNamespace))
				Expect(chaniList).ToNot(ContainElement(paulNamespace))
				Expect(chaniList).To(ContainElement(chaniNamespace))
			})

			It("recovers when there are kube write failures", func(ctx context.Context) {
				// paul creates his namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())

				// make kube write fail for chani's namespace, spicedb write will have
				// succeeded
				if lockMode == proxyrule.PessimisticLockMode {
					// the locking version retries if the connection fails
					failpoints.EnableFailPoint("panicKubeWrite", distributedtx.MaxKubeAttempts+1)
				} else {
					failpoints.EnableFailPoint("panicKubeWrite", 1)
				}
				// Chani's write panics, but is retried
				Expect(CreateNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())

				// paul isn't able to create chanis namespace
				Expect(CreateNamespace(ctx, paulClient, chaniNamespace)).ToNot(BeNil())

				// paul can only get his namespace
				Expect(GetNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(GetNamespace(ctx, paulClient, chaniNamespace)).ToNot(BeNil())

				// chani can get her namespace - this indicates the workflow was retried and eventually succeeded
				Expect(GetNamespace(ctx, chaniClient, paulNamespace)).ToNot(BeNil())
				Expect(GetNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())
			})

			It("recovers when there are kube delete failures", func(ctx context.Context) {
				// paul creates his pod
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreatePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())

				// make kube delete fail, spicedb write will have succeeded
				if lockMode == proxyrule.PessimisticLockMode {
					// the locking version retries if the connection fails
					failpoints.EnableFailPoint("panicKubeWrite", distributedtx.MaxKubeAttempts+1)
				} else {
					failpoints.EnableFailPoint("panicKubeWrite", 1)
				}
				// delete panics, but is retried
				Expect(DeletePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())

				// the pod is gone on subsequent calls
				Expect(k8serrors.IsUnauthorized(GetPod(ctx, paulClient, paulNamespace, paulPod))).To(BeTrue())
				Expect(k8serrors.IsNotFound(GetPod(ctx, adminClient, paulNamespace, paulPod))).To(BeTrue())
			})

			It("recovers when kube write succeeds but crashes", func(ctx context.Context) {
				// paul creates his namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())

				// make kube write succeed, but crash process before it can be recorded
				failpoints.EnableFailPoint("panicKubeReadResp", 1)
				Expect(CreateNamespace(ctx, chaniClient, chaniNamespace)).ToNot(BeNil())

				// Chani can get her namespace - the workflow has resolved the write
				// Pessimistic locking retried the kube request and got an "already exists" err
				// Optimistic locking checked kube and saw that the object already existed
				Expect(GetNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())
			})

			It("recovers when kube delete succeeds but crashes", func(ctx context.Context) {
				// paul creates his pod
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreatePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())

				// make kube delete succeed, but crash process before it can be recorded
				failpoints.EnableFailPoint("panicKubeReadResp", 1)
				Expect(DeletePod(ctx, paulClient, paulNamespace, paulPod)).ToNot(BeNil())

				// Make sure tuples are gone
				owners := GetAllTuples(ctx, &v1.RelationshipFilter{
					ResourceType:          "pod",
					OptionalResourceId:    paulNamespace + "/" + paulPod,
					OptionalRelation:      "creator",
					OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "user"},
				})
				Expect(len(owners)).To(BeZero())

				// the pod is gone on subsequent calls
				Expect(k8serrors.IsUnauthorized(GetPod(ctx, paulClient, paulNamespace, paulPod))).To(BeTrue())
				Expect(k8serrors.IsNotFound(GetPod(ctx, adminClient, paulNamespace, paulPod))).To(BeTrue())
			})

			It("prevents ownership stealing when crashing", func(ctx context.Context) {
				// paul creates chani's namespace, but crashes before returning
				failpoints.EnableFailPoint("panicKubeReadResp", 1)
				Expect(CreateNamespace(ctx, paulClient, chaniNamespace)).ToNot(BeNil())

				// chani attempts to create chani's namespace
				err := CreateNamespace(ctx, chaniClient, chaniNamespace)
				Expect(k8serrors.IsConflict(err) || k8serrors.IsAlreadyExists(err)).To(BeTrue())

				// Chani can't get her namespace - paul created it first and hasn't shared it
				Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())
			})

			It("prevents ownership stealing when retrying second write", func(ctx context.Context) {
				// paul creates chani's namespace,
				Expect(CreateNamespace(ctx, paulClient, chaniNamespace)).To(Succeed())

				// chani attempts to create chani's namespace, but crashes before returning
				failpoints.EnableFailPoint("panicKubeReadResp", 1)
				err := CreateNamespace(ctx, chaniClient, chaniNamespace)
				Expect(k8serrors.IsConflict(err) || k8serrors.IsAlreadyExists(err)).To(BeTrue())

				// Chani can't get her namespace - paul created it first and hasn't shared it
				Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())
			})

			It("recovers writes when there are spicedb write failures", func(ctx context.Context) {
				// paul creates his namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())

				// make spicedb write crash on chani's namespace write, eventually succeeds
				failpoints.EnableFailPoint("panicWriteSpiceDB", 1)
				Expect(CreateNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())

				// paul is unable to create chani's namespace as it's already claimed
				Expect(CreateNamespace(ctx, paulClient, chaniNamespace)).ToNot(BeNil())

				// Check Chani is able to get namespace
				Expect(GetNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())

				// confirm the relationship exists
				Expect(len(GetAllTuples(ctx, &v1.RelationshipFilter{
					ResourceType:          "namespace",
					OptionalResourceId:    chaniNamespace,
					OptionalRelation:      "creator",
					OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "user", OptionalSubjectId: "chani"},
				}))).ToNot(BeZero())
			})

			It("recovers deletes when there are spicedb write failures", func(ctx context.Context) {
				// paul creates his namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreatePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())

				// make spicedb write crash on pod delete
				failpoints.EnableFailPoint("panicWriteSpiceDB", 1)
				Expect(DeletePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())

				// chani is able to create pauls's pod, the delete succeeded
				Expect(CreatePod(ctx, chaniClient, paulNamespace, paulPod)).To(Succeed())

				// confirm the relationship exists
				owners := GetAllTuples(ctx, &v1.RelationshipFilter{
					ResourceType:          "pod",
					OptionalResourceId:    paulNamespace + "/" + paulPod,
					OptionalRelation:      "creator",
					OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "user"},
				})
				Expect(len(owners)).ToNot(BeZero())
				Expect(owners[0].Relationship.Subject.Object.ObjectId).To(Equal("chani"))
			})

			It("recovers writes when spicedb write succeeds but crashes", func(ctx context.Context) {
				// paul creates his namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())

				// make spicedb write crash on chani's namespace write
				failpoints.EnableFailPoint("panicSpiceDBReadResp", 1)
				err := CreateNamespace(ctx, chaniClient, chaniNamespace)
				Expect(err).ToNot(BeNil())
				// pessimistic locking reports a conflict, optimistic locking reports already exists
				Expect(k8serrors.IsConflict(err) || k8serrors.IsAlreadyExists(err)).To(BeTrue())

				// paul creates chani's namespace so that the namespace exists
				Expect(CreateNamespace(ctx, paulClient, chaniNamespace)).To(Succeed())

				// check that chani can't get her namespace, indirectly showing
				// that the spicedb write was rolled back
				Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())

				// confirm the relationship doesn't exist
				Expect(len(GetAllTuples(ctx, &v1.RelationshipFilter{
					ResourceType:          "namespace",
					OptionalResourceId:    chaniNamespace,
					OptionalRelation:      "creator",
					OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "user", OptionalSubjectId: "chani"},
				}))).To(BeZero())

				// confirm paul can get the namespace
				Expect(GetNamespace(ctx, paulClient, chaniNamespace)).To(Succeed())
			})

			It("recovers deletes when spicedb write succeeds but crashes", func(ctx context.Context) {
				// paul creates his namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreatePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())

				// chani can't create the same pod
				Expect(k8serrors.IsAlreadyExists(CreatePod(ctx, chaniClient, paulNamespace, paulPod))).To(BeTrue())
				fmt.Println(GetPod(ctx, chaniClient, paulNamespace, paulPod))
				Expect(k8serrors.IsUnauthorized(GetPod(ctx, chaniClient, paulNamespace, paulPod))).To(BeTrue())

				// make spicedb write crash on pod delete
				failpoints.EnableFailPoint("panicSpiceDBReadResp", 1)
				err := DeletePod(ctx, paulClient, paulNamespace, paulPod)
				if lockMode == proxyrule.OptimisticLockMode {
					Expect(err).To(Succeed())
				} else {
					Expect(k8serrors.IsConflict(err) || k8serrors.IsUnauthorized(err)).To(BeTrue())
					// paul sees the request fail, so he tries again:
					Expect(DeletePod(ctx, paulClient, paulNamespace, paulPod)).To(Succeed())
				}

				// chani can now re-create paul's pod and take ownership
				Expect(CreatePod(ctx, chaniClient, paulNamespace, paulPod)).To(Succeed())

				// check that paul can't get the pod that chani created
				Expect(k8serrors.IsUnauthorized(GetPod(ctx, paulClient, paulNamespace, paulPod))).To(BeTrue())

				// confirm the relationship exists
				owners := GetAllTuples(ctx, &v1.RelationshipFilter{
					ResourceType:          "pod",
					OptionalResourceId:    paulNamespace + "/" + paulPod,
					OptionalRelation:      "creator",
					OptionalSubjectFilter: &v1.SubjectFilter{SubjectType: "user"},
				})
				Expect(len(owners)).ToNot(BeZero())
				Expect(owners[0].Relationship.Subject.Object.ObjectId).To(Equal("chani"))

				// confirm chani can get the pod
				Expect(GetPod(ctx, chaniClient, paulNamespace, paulPod)).To(Succeed())
			})

			It("ensures only one write at a time happens for a given object", MustPassRepeatedly(5), func(ctx context.Context) {
				// both attempt to create the namespace
				start := make(chan struct{})
				errs := make(chan error, 2)

				var wg errgroup.Group

				// in theory, these two requests could be run serially, but in
				// practice they seem to always actually run in parallel as
				// intended.
				wg.Go(func() error {
					defer GinkgoRecover()
					<-start
					errs <- CreateNamespace(ctx, paulClient, paulNamespace)
					return nil
				})
				wg.Go(func() error {
					defer GinkgoRecover()
					<-start
					errs <- CreateNamespace(ctx, chaniClient, paulNamespace)
					return nil
				})
				start <- struct{}{}
				start <- struct{}{}
				_ = wg.Wait()
				close(errs)

				allErrs := make([]error, 0)
				for err := range errs {
					if err != nil {
						allErrs = append(allErrs, err)
					}
				}
				Expect(len(allErrs)).To(Equal(1))
				Expect(k8serrors.IsConflict(allErrs[0]) || // pessimistic lock
					k8serrors.IsAlreadyExists(allErrs[0]), // optimistic lock
				).To(BeTrue())
			})

			It("revokes access during watch", func(ctx context.Context) {
				Expect(CreateNamespace(ctx, adminClient, sharedNamespace)).To(Succeed())
				Expect(CreatePod(ctx, chaniClient, sharedNamespace, chaniPod)).To(Succeed())

				// Chani deletes her pod, which will remove her access
				// to that pod in spicedb
				Expect(DeletePod(ctx, chaniClient, sharedNamespace, chaniPod)).To(Succeed())

				Eventually(func(g Gomega) {
					fmt.Println(GetPod(ctx, adminClient, sharedNamespace, chaniPod))
					g.Expect(GetPod(ctx, adminClient, sharedNamespace, chaniPod)).To(Not(Succeed()))
				}).Should(Succeed())

				// start a watch waiting for one result, but expect 0 results
				// after the watch times out
				var wg errgroup.Group
				wg.Go(func() error {
					defer GinkgoRecover()
					Expect(len(WatchPods(ctx, chaniClient, sharedNamespace, 1, 2*time.Second))).To(BeZero())
					return nil
				})

				// paul should get an event for the pod he creates
				wg.Go(func() error {
					defer GinkgoRecover()
					Expect(len(WatchPods(ctx, paulClient, sharedNamespace, 1, 2*time.Second))).To(Equal(1))
					return nil
				})

				// Paul creates chani's pod, will generate kube events that
				// Chani shouldn't see
				Expect(CreatePod(ctx, paulClient, sharedNamespace, chaniPod)).To(Succeed())

				// wait for Chani's watch to time out, which means she didn't
				// see the events from pauls writes
				wg.Wait()
			})
		}

		When("optimistic locking is used", func() {
			BeforeEach(func() {
				*proxySrv.Matcher = testOptimisticMatcher()
				lockMode = proxyrule.OptimisticLockMode
			})
			AssertDualWriteBehavior()
		})

		When("pessimistic locking is used", func() {
			BeforeEach(func() {
				*proxySrv.Matcher = testPessimisticMatcher()
				lockMode = proxyrule.PessimisticLockMode
			})
			AssertDualWriteBehavior()
		})

		When("no rules match the request", func() {
			It("returns unauthenticated error", func(ctx context.Context) {
				*proxySrv.Matcher = rules.MatcherFunc(func(match *request.RequestInfo) []*rules.RunnableRule {
					return nil
				})
				Expect(GetNamespace(ctx, paulClient, paulNamespace)).NotTo(Succeed())
			})
		})

		When("PostChecks are used in rules", func() {
			It("allows get operations when postchecks pass", func(ctx context.Context) {
				getNamespaceWithPostCheck := proxyrule.Config{
					Spec: proxyrule.Spec{
						Matches: []proxyrule.Match{{
							GroupVersion: "v1",
							Resource:     "namespaces",
							Verbs:        []string{"get"},
						}},
						Checks: []proxyrule.StringOrTemplate{{
							Template: "namespace:{{name}}#view@user:{{user.name}}",
						}},
						PostChecks: []proxyrule.StringOrTemplate{{
							Template: "namespace:{{name}}#edit@user:{{user.name}}",
						}},
					},
				}

				// Set up matcher with postcheck rule
				matcher, err := rules.NewMapMatcher([]proxyrule.Config{
					createNamespace(),
					getNamespaceWithPostCheck,
				})
				Expect(err).To(Succeed())
				*proxySrv.Matcher = matcher

				// Create namespace and required relationships
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())

				// Add viewer relation for Paul (so postcheck will pass)
				WriteTuples(ctx, []*v1.Relationship{{
					Resource: &v1.ObjectReference{ObjectType: "namespace", ObjectId: paulNamespace},
					Relation: "viewer",
					Subject:  &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: "paul"}},
				}})

				// Paul should be able to get the namespace (postchecks pass)
				Expect(GetNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
			})

			It("blocks get operations when postchecks fail", func(ctx context.Context) {
				getNamespaceWithPostCheck := proxyrule.Config{
					Spec: proxyrule.Spec{
						Matches: []proxyrule.Match{{
							GroupVersion: "v1",
							Resource:     "namespaces",
							Verbs:        []string{"get"},
						}},
						Checks: []proxyrule.StringOrTemplate{{
							Template: "namespace:{{name}}#view@user:{{user.name}}",
						}},
						PostChecks: []proxyrule.StringOrTemplate{{
							Template: "namespace:{{name}}#no_one_at_all@user:{{user.name}}",
						}},
					},
				}

				// Set up matcher with postcheck rule
				matcher, err := rules.NewMapMatcher([]proxyrule.Config{
					createNamespace(),
					getNamespaceWithPostCheck,
				})
				Expect(err).To(Succeed())
				*proxySrv.Matcher = matcher

				// Create namespace - this gives Paul view permission but not no_one_at_all permission
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())

				// Paul should NOT be able to get the namespace (postchecks fail because he doesn't have no_one_at_all permission)
				Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, paulClient, paulNamespace))).To(BeTrue())
			})
		})

		When("rules with CEL 'if' conditions are used", func() {
			BeforeEach(func() {
				*proxySrv.Matcher = testCELIfMatcher()
				lockMode = proxyrule.OptimisticLockMode
			})

			It("allows access when CEL conditions are met", func(ctx context.Context) {
				// Create namespace as admin user (should meet the CEL condition)
				Expect(CreateNamespace(ctx, adminClient, sharedNamespace)).To(Succeed())

				// Admin should be able to get the namespace (CEL condition allows admin)
				Expect(GetNamespace(ctx, adminClient, sharedNamespace)).To(Succeed())

				// Paul should not be able to get the namespace (CEL condition blocks non-admin users)
				Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, paulClient, sharedNamespace))).To(BeTrue())
			})

			It("denies access when CEL conditions are not met", func(ctx context.Context) {
				// Paul tries to create a pod in non-default namespace (should be blocked by CEL)
				Expect(CreateNamespace(ctx, adminClient, paulNamespace)).To(Succeed())

				// Paul should not be able to create pods outside default namespace
				Expect(k8serrors.IsUnauthorized(CreatePod(ctx, paulClient, paulNamespace, paulPod))).To(BeTrue())

				// Create default namespace for testing (ignore if it already exists)
				err := CreateNamespace(ctx, adminClient, "default")
				if err != nil && !k8serrors.IsAlreadyExists(err) {
					Expect(err).To(Succeed())
				}

				// Paul should be able to create pods in default namespace
				Expect(CreatePod(ctx, paulClient, "default", paulPod)).To(Succeed())
			})

			It("evaluates multiple CEL conditions correctly", func(ctx context.Context) {
				// Create namespaces (ignore if default already exists)
				err := CreateNamespace(ctx, adminClient, "default")
				if err != nil && !k8serrors.IsAlreadyExists(err) {
					Expect(err).To(Succeed())
				}

				// Paul should be able to create pods in default namespace
				Expect(CreatePod(ctx, paulClient, "default", paulPod)).To(Succeed())

				// Paul should be able to perform read in the default namespace
				Expect(GetPod(ctx, paulClient, "default", paulPod)).To(Succeed())

				// Paul should not be able to perform write operations outside default namespace
				Expect(k8serrors.IsUnauthorized(DeletePod(ctx, paulClient, paulNamespace, paulPod))).To(BeTrue())
			})

			When("tupleset is used with pod labels", func() {
				BeforeEach(func() {
					createPodWithLabelTupleSet := proxyrule.Config{
						Spec: proxyrule.Spec{
							Locking: proxyrule.OptimisticLockMode,
							Matches: []proxyrule.Match{{
								GroupVersion: "v1",
								Resource:     "pods",
								Verbs:        []string{"create"},
							}},
							Update: proxyrule.Update{
								CreateRelationships: []proxyrule.StringOrTemplate{
									{
										Template: "pod:{{namespacedName}}#creator@user:{{user.name}}",
									},
									{
										Template: "pod:{{namespacedName}}#namespace@namespace:{{namespace}}",
									},
									{
										// TupleSet for pod labels - creates a viewer relationship for each label key
										// Using multi-line pattern with explicit root assignment
										TupleSet: `let nsName = this.namespacedName
											let labelKeys = (this.object.metadata.labels | {}).keys()
											root = $labelKeys.map_each("pod:" + $nsName + "#viewer@user:" + this)`,
									},
								},
							},
						},
					}

					matcher, err := rules.NewMapMatcher([]proxyrule.Config{
						createNamespace(),
						createPodWithLabelTupleSet,
						getPod,
					})
					Expect(err).To(Succeed())
					*proxySrv.Matcher = matcher
					lockMode = proxyrule.OptimisticLockMode
				})

				It("creates relationships for each pod label", func(ctx context.Context) {
					// Create namespace first
					Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())

					// Create a pod with multiple labels
					podLabels := map[string]string{
						"app":         "web-server",
						"environment": "production",
						"team":        "backend",
						"version":     "v1.2.3",
					}

					pod := &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      paulPod,
							Namespace: paulNamespace,
							Labels:    podLabels,
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "nginx",
								Image: "nginx:1.14.2",
							}},
						},
					}

					_, err := paulClient.CoreV1().Pods(paulNamespace).Create(ctx, pod, metav1.CreateOptions{})
					Expect(err).To(Succeed())

					// Verify the basic creator relationship exists
					creatorRels := GetAllTuples(ctx, &v1.RelationshipFilter{
						ResourceType:       "pod",
						OptionalResourceId: fmt.Sprintf("%s/%s", paulNamespace, paulPod),
						OptionalRelation:   "creator",
					})
					Expect(len(creatorRels)).To(Equal(1))
					Expect(creatorRels[0].Relationship.Subject.Object.ObjectId).To(Equal("paul"))

					// Verify the namespace relationship exists
					namespaceRels := GetAllTuples(ctx, &v1.RelationshipFilter{
						ResourceType:       "pod",
						OptionalResourceId: fmt.Sprintf("%s/%s", paulNamespace, paulPod),
						OptionalRelation:   "namespace",
					})
					Expect(len(namespaceRels)).To(Equal(1))
					Expect(namespaceRels[0].Relationship.Subject.Object.ObjectId).To(Equal(paulNamespace))

					// Verify tupleset created relationships for each label key
					viewerRels := GetAllTuples(ctx, &v1.RelationshipFilter{
						ResourceType:       "pod",
						OptionalResourceId: fmt.Sprintf("%s/%s", paulNamespace, paulPod),
						OptionalRelation:   "viewer",
					})
					Expect(len(viewerRels)).To(Equal(4)) // One for each label key

					// Check that each label key has a corresponding viewer relationship
					labelKeys := []string{"app", "environment", "team", "version"}
					actualViewers := make([]string, len(viewerRels))
					for i, rel := range viewerRels {
						actualViewers[i] = rel.Relationship.Subject.Object.ObjectId
					}

					for _, expectedKey := range labelKeys {
						Expect(actualViewers).To(ContainElement(expectedKey))
					}
				})
			})
		})
	})
})

var (
	createTestResource = func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1",
				Resource:     "testresources",
				Verbs:        []string{"create"},
			}},
			Update: proxyrule.Update{
				CreateRelationships: []proxyrule.StringOrTemplate{{
					Template: "testresource:{{namespacedName}}#creator@user:{{user.name}}",
				}, {
					Template: "testresource:{{name}}#namespace@namespace:{{namespace}}",
				}},
			},
		}}
	}

	deleteTestResource = func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1",
				Resource:     "testresources",
				Verbs:        []string{"delete"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "testresource:{{namespacedName}}#edit@user:{{user.name}}",
			}},
			Update: proxyrule.Update{
				DeleteRelationships: []proxyrule.StringOrTemplate{{
					Template: "testresource:{{namespacedName}}#creator@user:{{user.name}}",
				}, {
					Template: "testresource:{{name}}#namespace@namespace:{{namespace}}",
				}},
			},
		}}
	}

	getTestResource = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1",
				Resource:     "testresources",
				Verbs:        []string{"get"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "testresource:{{namespacedName}}#view@user:{{user.name}}",
			}},
		},
	}

	updateTestResource = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1",
				Resource:     "testresources",
				Verbs:        []string{"update", "patch"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "testresource:{{namespacedName}}#edit@user:{{user.name}}",
			}},
			Update: proxyrule.Update{
				TouchRelationships: []proxyrule.StringOrTemplate{{
					Template: "testresource:{{namespacedName}}#creator@user:{{user.name}}",
				}},
			},
		},
	}

	listWatchTestResource = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1",
				Resource:     "testresources",
				Verbs:        []string{"list", "watch"},
			}},
			PreFilters: []proxyrule.PreFilter{{
				FromObjectIDNamespaceExpr: "{{split_namespace(resourceId)}}",
				FromObjectIDNameExpr:      "{{split_name(resourceId)}}",
				LookupMatchingResources:   &proxyrule.StringOrTemplate{Template: "testresource:$#view@user:{{user.name}}"},
			}},
		},
	}

	createAnotherTestResource = func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1",
				Resource:     "anothertestresources",
				Verbs:        []string{"create"},
			}},
			Update: proxyrule.Update{
				CreateRelationships: []proxyrule.StringOrTemplate{{
					Template: "testresource:{{namespacedName}}#creator@user:{{user.name}}",
				}, {
					Template: "testresource:{{name}}#namespace@namespace:{{namespace}}",
				}},
			},
		}}
	}

	deleteAnotherTestResource = func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1",
				Resource:     "anothertestresources",
				Verbs:        []string{"delete"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "testresource:{{namespacedName}}#edit@user:{{user.name}}",
			}},
			Update: proxyrule.Update{
				DeleteByFilter: []proxyrule.StringOrTemplate{{
					Template: "testresource:{{namespacedName}}#$resourceRelation@$subjectType:$subjectID",
				}, {
					Template: "testresource:{{name}}#$resourceRelation@$subjectType:$subjectID",
				}},
			},
		}}
	}

	getAnotherTestResource = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1",
				Resource:     "anothertestresources",
				Verbs:        []string{"get"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "testresource:{{namespacedName}}#view@user:{{user.name}}",
			}},
		},
	}

	listWatchAnotherTestResource = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "example.com/v1",
				Resource:     "anothertestresources",
				Verbs:        []string{"list", "watch"},
			}},
			PostFilters: []proxyrule.PostFilter{{
				CheckPermissionTemplate: &proxyrule.StringOrTemplate{Template: "testresource:{{namespacedName}}#view@user:{{user.name}}"},
			}},
		},
	}

	createNamespace = func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "namespaces",
				Verbs:        []string{"create"},
			}},
			Update: proxyrule.Update{
				CreateRelationships: []proxyrule.StringOrTemplate{{
					Template: "namespace:{{name}}#creator@user:{{user.name}}",
				}, {
					Template: "namespace:{{name}}#cluster@cluster:cluster",
				}},
			},
		}}
	}

	deleteNamespace = func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "namespaces",
				Verbs:        []string{"delete"},
			}},
			Update: proxyrule.Update{
				DeleteRelationships: []proxyrule.StringOrTemplate{{
					Template: "namespace:{{name}}#creator@user:{{user.name}}",
				}, {
					Template: "namespace:{{name}}#cluster@cluster:cluster",
				}},
			},
		}}
	}

	getNamespace = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "namespaces",
				Verbs:        []string{"get"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "namespace:{{name}}#view@user:{{user.name}}",
			}},
		},
	}

	listWatchNamespace = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "namespaces",
				Verbs:        []string{"list", "watch"},
			}},
			PreFilters: []proxyrule.PreFilter{{
				FromObjectIDNameExpr:    "{{resourceId}}",
				LookupMatchingResources: &proxyrule.StringOrTemplate{Template: "namespace:$#view@user:{{user.name}}"},
			}},
		},
	}

	createPod = func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "pods",
				Verbs:        []string{"create"},
			}},
			Update: proxyrule.Update{
				CreateRelationships: []proxyrule.StringOrTemplate{{
					Template: "pod:{{namespacedName}}#creator@user:{{user.name}}",
				}, {
					Template: "pod:{{name}}#namespace@namespace:{{namespace}}",
				}},
			},
		}}
	}

	deletePod = func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "pods",
				Verbs:        []string{"delete"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "pod:{{namespacedName}}#edit@user:{{user.name}}",
			}},
			Update: proxyrule.Update{
				DeleteRelationships: []proxyrule.StringOrTemplate{{
					Template: "pod:{{namespacedName}}#creator@user:{{user.name}}",
				}, {
					Template: "pod:{{name}}#namespace@namespace:{{namespace}}",
				}},
			},
		}}
	}

	getPod = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "pods",
				Verbs:        []string{"get"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "pod:{{namespacedName}}#view@user:{{user.name}}",
			}},
		},
	}

	updatePod = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "pods",
				Verbs:        []string{"update", "patch"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "pod:{{namespacedName}}#edit@user:{{user.name}}",
			}},
		},
	}

	listWatchPod = proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "pods",
				Verbs:        []string{"list", "watch"},
			}},
			PreFilters: []proxyrule.PreFilter{{
				FromObjectIDNamespaceExpr: "{{split_namespace(resourceId)}}",
				FromObjectIDNameExpr:      "{{split_name(resourceId)}}",
				LookupMatchingResources:   &proxyrule.StringOrTemplate{Template: "pod:$#view@user:{{user.name}}"},
			}},
		},
	}
)

func testOptimisticMatcher() rules.MapMatcher {
	matcher, err := rules.NewMapMatcher([]proxyrule.Config{
		createNamespace(),
		deleteNamespace(),
		getNamespace,
		listWatchNamespace,
		createPod(),
		deletePod(),
		getPod,
		updatePod,
		listWatchPod,
		createTestResource(),
		deleteTestResource(),
		getTestResource,
		updateTestResource,
		listWatchTestResource,
		getAnotherTestResource,
		listWatchAnotherTestResource,
		createAnotherTestResource(),
		deleteAnotherTestResource(),
	})
	Expect(err).To(Succeed())
	return matcher
}

func testPessimisticMatcher() rules.MapMatcher {
	pessimisticCreateNamespace := createNamespace()
	pessimisticCreateNamespace.Update.PreconditionDoesNotExist = []proxyrule.StringOrTemplate{{
		Template: "namespace:{{object.metadata.name}}#cluster@cluster:cluster",
	}}
	pessimisticCreateNamespace.Locking = proxyrule.PessimisticLockMode

	pessimisticDeleteNamespace := deleteNamespace()
	pessimisticDeleteNamespace.Locking = proxyrule.PessimisticLockMode

	pessimisticCreatePod := createPod()
	pessimisticCreatePod.Locking = proxyrule.PessimisticLockMode
	pessimisticCreatePod.Update.PreconditionDoesNotExist = []proxyrule.StringOrTemplate{{
		Template: "pod:{{object.metadata.name}}#namespace@namespace:{{request.namespace}}",
	}}
	pessimisticDeletePod := deletePod()
	pessimisticDeletePod.Locking = proxyrule.PessimisticLockMode

	pessimisticCreateTestResource := createTestResource()
	pessimisticCreateTestResource.Locking = proxyrule.PessimisticLockMode
	pessimisticCreateTestResource.Update.PreconditionDoesNotExist = []proxyrule.StringOrTemplate{{
		Template: "testresource:{{object.metadata.name}}#namespace@namespace:{{request.namespace}}",
	}}
	pessimisticDeleteTestResource := deleteTestResource()
	pessimisticDeleteTestResource.Locking = proxyrule.PessimisticLockMode

	pessimisticCreateAnotherTestResource := createAnotherTestResource()
	pessimisticCreateAnotherTestResource.Locking = proxyrule.PessimisticLockMode
	pessimisticCreateAnotherTestResource.Update.PreconditionDoesNotExist = []proxyrule.StringOrTemplate{{
		Template: "testresource:{{object.metadata.name}}#namespace@namespace:{{request.namespace}}",
	}}

	pessimisticDeleteAnotherTestResource := deleteAnotherTestResource()
	pessimisticDeleteAnotherTestResource.Locking = proxyrule.PessimisticLockMode

	matcher, err := rules.NewMapMatcher([]proxyrule.Config{
		pessimisticCreateNamespace,
		pessimisticDeleteNamespace,
		getNamespace,
		listWatchNamespace,
		pessimisticCreatePod,
		pessimisticDeletePod,
		getPod,
		updatePod,
		listWatchPod,
		pessimisticCreateTestResource,
		pessimisticDeleteTestResource,
		getTestResource,
		updateTestResource,
		listWatchTestResource,
		getAnotherTestResource,
		listWatchAnotherTestResource,
		pessimisticCreateAnotherTestResource,
		pessimisticDeleteAnotherTestResource,
	})
	Expect(err).To(Succeed())
	return matcher
}

func testCELIfMatcher() rules.MapMatcher {
	celCreateNamespace := func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "namespaces",
				Verbs:        []string{"create"},
			}},
			If: []string{
				"user.name == 'admin' || 'system:masters' in user.groups",
			},
			Update: proxyrule.Update{
				CreateRelationships: []proxyrule.StringOrTemplate{{
					Template: "namespace:{{name}}#creator@user:{{user.name}}",
				}, {
					Template: "namespace:{{name}}#cluster@cluster:cluster",
				}},
			},
		}}
	}

	celGetNamespace := proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "namespaces",
				Verbs:        []string{"get"},
			}},
			If: []string{
				"user.name == 'admin' || 'system:masters' in user.groups",
			},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "namespace:{{name}}#view@user:{{user.name}}",
			}},
		},
	}

	celCreatePod := func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "pods",
				Verbs:        []string{"create"},
			}},
			If: []string{
				"user.name == 'admin' || resourceNamespace == 'default'",
			},
			Update: proxyrule.Update{
				CreateRelationships: []proxyrule.StringOrTemplate{{
					Template: "pod:{{namespacedName}}#creator@user:{{user.name}}",
				}, {
					Template: "pod:{{name}}#namespace@namespace:{{namespace}}",
				}},
			},
		}}
	}

	celGetPod := proxyrule.Config{
		Spec: proxyrule.Spec{
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "pods",
				Verbs:        []string{"get"},
			}},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "pod:{{namespacedName}}#view@user:{{user.name}}",
			}},
		},
	}

	celDeletePod := func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "pods",
				Verbs:        []string{"delete"},
			}},
			If: []string{
				"user.name == 'admin' || resourceNamespace == 'default'",
			},
			Checks: []proxyrule.StringOrTemplate{{
				Template: "pod:{{namespacedName}}#edit@user:{{user.name}}",
			}},
			Update: proxyrule.Update{
				DeleteRelationships: []proxyrule.StringOrTemplate{{
					Template: "pod:{{namespacedName}}#creator@user:{{user.name}}",
				}, {
					Template: "pod:{{name}}#namespace@namespace:{{namespace}}",
				}},
			},
		}}
	}

	matcher, err := rules.NewMapMatcher([]proxyrule.Config{
		celCreateNamespace(),
		celGetNamespace,
		celCreatePod(),
		celGetPod,
		celDeletePod(),
	})
	Expect(err).To(Succeed())
	return matcher
}
