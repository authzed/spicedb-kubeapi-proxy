//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/authz/distributedtx"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/config/proxyrule"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/failpoints"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/rules"
)

var _ = Describe("Proxy", func() {
	When("there are two users", func() {
		var paulClient, chaniClient, adminClient kubernetes.Interface
		var paulNamespace, chaniNamespace, sharedNamespace string
		var chaniPod, paulPod string
		var lockMode proxyrule.LockMode

		BeforeEach(func() {
			paulRestConfig, err := clientcmd.NewDefaultClientConfig(
				*clientCA.GenerateUserConfig("paul"), nil,
			).ClientConfig()
			Expect(err).To(Succeed())
			paulClient, err = kubernetes.NewForConfig(paulRestConfig)
			Expect(err).To(Succeed())

			chaniRestConfig, err := clientcmd.NewDefaultClientConfig(
				*clientCA.GenerateUserConfig("chani"), nil,
			).ClientConfig()
			Expect(err).To(Succeed())
			chaniClient, err = kubernetes.NewForConfig(chaniRestConfig)
			Expect(err).To(Succeed())

			adminClient, err = kubernetes.NewForConfig(adminUser.Config())
			Expect(err).To(Succeed())

			paulNamespace = names.SimpleNameGenerator.GenerateName("paul-")
			chaniNamespace = names.SimpleNameGenerator.GenerateName("chani-")

			// pods are used for tests that require deletes, since the GC
			// controller can clean them up in tests (namespaces can't be GCd)
			sharedNamespace = names.SimpleNameGenerator.GenerateName("shared-")
			paulPod = names.SimpleNameGenerator.GenerateName("paul-pod-")
			chaniPod = names.SimpleNameGenerator.GenerateName("chani-pod-")
		})

		AfterEach(func(ctx context.Context) {
			orphan := metav1.DeletePropagationOrphan
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, paulNamespace, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, chaniNamespace, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminClient.CoreV1().Pods(sharedNamespace).Delete(ctx, paulPod, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminClient.CoreV1().Pods(sharedNamespace).Delete(ctx, chaniPod, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, sharedNamespace, metav1.DeleteOptions{PropagationPolicy: &orphan})

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
		WatchPods := func(ctx context.Context, client kubernetes.Interface, namespace string, expected int, timeout time.Duration) []string {
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			got := make([]string, 0, expected)
			watcher, err := client.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{})
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

		JustBeforeEach(func(ctx context.Context) {
			// before every test, assert no access
			Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, paulClient, paulNamespace))).To(BeTrue())
			Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, paulClient, chaniNamespace))).To(BeTrue())
			Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, chaniClient, paulNamespace))).To(BeTrue())
			Expect(k8serrors.IsUnauthorized(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())
		})

		AssertDualWriteBehavior := func() {
			It("doesn't show users namespaces the other has created", func(ctx context.Context) {
				var wg sync.WaitGroup
				defer wg.Wait()
				wg.Add(2)
				go func() {
					defer GinkgoRecover()
					Expect(WatchNamespaces(ctx, paulClient, 1)).To(ContainElement(paulNamespace))
					wg.Done()
				}()
				go func() {
					defer GinkgoRecover()
					Expect(WatchNamespaces(ctx, chaniClient, 1)).To(ContainElement(chaniNamespace))
					wg.Done()
				}()

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
					fmt.Println(err)
					Expect(k8serrors.IsUnauthorized(err)).To(BeTrue())
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

				var wg sync.WaitGroup
				wg.Add(2)

				// in theory, these two requests could be run serially, but in
				// practice they seem to always actually run in parallel as
				// intended.
				go func() {
					defer GinkgoRecover()
					<-start
					errs <- CreateNamespace(ctx, paulClient, paulNamespace)
					wg.Done()
				}()
				go func() {
					defer GinkgoRecover()
					<-start
					errs <- CreateNamespace(ctx, chaniClient, paulNamespace)
					wg.Done()
				}()
				start <- struct{}{}
				start <- struct{}{}
				wg.Wait()
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
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					Expect(len(WatchPods(ctx, chaniClient, sharedNamespace, 1, 2*time.Second))).To(BeZero())
					wg.Done()
				}()

				// paul should get an event for the pod he creates
				wg.Add(1)
				go func() {
					defer GinkgoRecover()
					Expect(len(WatchPods(ctx, paulClient, sharedNamespace, 1, 2*time.Second))).To(Equal(1))
					wg.Done()
				}()

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
	})
})

var (
	createNamespace = func() proxyrule.Config {
		return proxyrule.Config{Spec: proxyrule.Spec{
			Locking: proxyrule.OptimisticLockMode,
			Matches: []proxyrule.Match{{
				GroupVersion: "v1",
				Resource:     "namespaces",
				Verbs:        []string{"create"},
			}},
			Writes: []proxyrule.StringOrTemplate{{
				Template: "namespace:{{name}}#creator@user:{{user.Name}}",
			}, {
				Template: "namespace:{{name}}#cluster@cluster:cluster",
			}},
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
			Writes: []proxyrule.StringOrTemplate{{
				Template: "namespace:{{name}}#creator@user:{{user.Name}}",
			}, {
				Template: "namespace:{{name}}#cluster@cluster:cluster",
			}},
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
				Template: "namespace:{{name}}#view@user:{{user.Name}}",
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
				Name:       "resourceId",
				ByResource: &proxyrule.StringOrTemplate{Template: "namespace:*#view@user:{{user.Name}}"},
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
			Writes: []proxyrule.StringOrTemplate{{
				Template: "pod:{{namespacedName}}#creator@user:{{user.Name}}",
			}, {
				Template: "pod:{{name}}#namespace@namespace:{{namespace}}",
			}},
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
			Writes: []proxyrule.StringOrTemplate{{
				Template: "pod:{{namespacedName}}#creator@user:{{user.Name}}",
			}, {
				Template: "pod:{{name}}#namespace@namespace:{{namespace}}",
			}},
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
				Template: "pod:{{namespacedName}}#view@user:{{user.Name}}",
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
				Namespace:  "splitNamespace(resourceId)",
				Name:       "splitName(resourceId)",
				ByResource: &proxyrule.StringOrTemplate{Template: "pod:*#view@user:{{user.Name}}"},
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
		listWatchPod,
	})
	Expect(err).To(Succeed())
	return matcher
}

func testPessimisticMatcher() rules.MapMatcher {
	pessimisticCreateNamespace := createNamespace()
	pessimisticCreateNamespace.MustNot = []proxyrule.StringOrTemplate{{
		Template: "namespace:{{object.metadata.name}}#cluster@cluster:cluster",
	}}
	pessimisticCreateNamespace.Locking = proxyrule.PessimisticLockMode

	pessimisticDeleteNamespace := deleteNamespace()
	pessimisticDeleteNamespace.Locking = proxyrule.PessimisticLockMode

	pessimisticCreatePod := createPod()
	pessimisticCreatePod.Locking = proxyrule.PessimisticLockMode
	pessimisticCreatePod.MustNot = []proxyrule.StringOrTemplate{{
		Template: "pod:{{object.metadata.name}}#namespace@namespace:{{request.Namespace}}",
	}}
	pessimisticDeletePod := deletePod()
	pessimisticDeletePod.Locking = proxyrule.PessimisticLockMode

	matcher, err := rules.NewMapMatcher([]proxyrule.Config{
		pessimisticCreateNamespace,
		pessimisticDeleteNamespace,
		getNamespace,
		listWatchNamespace,
		pessimisticCreatePod,
		pessimisticDeletePod,
		getPod,
		listWatchPod,
	})
	Expect(err).To(Succeed())
	return matcher
}
