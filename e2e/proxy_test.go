//go:build e2e

package e2e

import (
	"context"
	"sync"

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

	"github.com/authzed/spicedb-kubeapi-proxy/pkg/failpoints"
	"github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy/distributedtx"
)

var _ = Describe("Proxy", func() {
	When("there are two users", func() {
		var paulClient, chaniClient, adminClient kubernetes.Interface
		var paulNamespace, chaniNamespace string

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
		})

		AfterEach(func(ctx context.Context) {
			orphan := metav1.DeletePropagationOrphan
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, paulNamespace, metav1.DeleteOptions{PropagationPolicy: &orphan})
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, chaniNamespace, metav1.DeleteOptions{PropagationPolicy: &orphan})

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

		JustBeforeEach(func(ctx context.Context) {
			// before every test, assert no access
			Expect(k8serrors.IsNotFound(GetNamespace(ctx, paulClient, paulNamespace))).To(BeTrue())
			Expect(k8serrors.IsNotFound(GetNamespace(ctx, paulClient, chaniNamespace))).To(BeTrue())
			Expect(k8serrors.IsNotFound(GetNamespace(ctx, chaniClient, paulNamespace))).To(BeTrue())
			Expect(k8serrors.IsNotFound(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())
		})

		AssertDualWriteBehavior := func() {
			It("doesn't show users namespaces the other has created", func(ctx context.Context) {
				// each creates their respective namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(CreateNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())

				// each can get their respective namespace
				Expect(GetNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(GetNamespace(ctx, chaniClient, chaniNamespace)).To(Succeed())

				// neither can get each other's namespace
				Expect(k8serrors.IsNotFound(GetNamespace(ctx, paulClient, chaniNamespace))).To(BeTrue())
				Expect(k8serrors.IsNotFound(GetNamespace(ctx, chaniClient, paulNamespace))).To(BeTrue())

				// neither can see each other's namespace in the list
				paulList := ListNamespaces(ctx, paulClient)
				chaniList := ListNamespaces(ctx, chaniClient)
				Expect(paulList).ToNot(ContainElement(chaniNamespace))
				Expect(paulList).To(ContainElement(paulNamespace))
				Expect(chaniList).ToNot(ContainElement(paulNamespace))
				Expect(chaniList).To(ContainElement(chaniNamespace))
			})

			It("cleans up dual writes on kube failures", func(ctx context.Context) {
				// paul creates his namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())

				// make kube write fail for chani's namespace, spicedb write will have
				// succeeded
				if proxySrv.LockMode == distributedtx.StrategyPessimisticWriteToSpiceDBAndKube {
					// the locking version retries if the connection fails
					failpoints.EnableFailPoint("panicKubeWrite", distributedtx.MaxKubeAttempts+1)
				} else {
					failpoints.EnableFailPoint("panicKubeWrite", 1)
				}
				Expect(CreateNamespace(ctx, chaniClient, chaniNamespace)).ToNot(BeNil())

				// paul creates chani's namespace
				Expect(CreateNamespace(ctx, paulClient, chaniNamespace)).To(Succeed())

				// paul can get both namespaces
				Expect(GetNamespace(ctx, paulClient, paulNamespace)).To(Succeed())
				Expect(GetNamespace(ctx, paulClient, chaniNamespace)).To(Succeed())

				// chani can't get her namespace - this indicates the spicedb write was rolled back
				// from the failed dual write above
				Expect(k8serrors.IsNotFound(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())
			})

			It("recovers dual writes when kube write succeeds but crashes", func(ctx context.Context) {
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

			It("prevents ownership stealing when crashing", func(ctx context.Context) {
				// paul creates chani's namespace, but crashes before returning
				failpoints.EnableFailPoint("panicKubeReadResp", 1)
				Expect(CreateNamespace(ctx, paulClient, chaniNamespace)).ToNot(BeNil())

				// chani attempts to create chani's namespace
				err := CreateNamespace(ctx, chaniClient, chaniNamespace)
				Expect(k8serrors.IsConflict(err) || k8serrors.IsAlreadyExists(err)).To(BeTrue())

				// Chani can't get her namespace - paul created it first and hasn't shared it
				Expect(k8serrors.IsNotFound(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())
			})

			It("prevents ownership stealing when retrying second write", func(ctx context.Context) {
				// paul creates chani's namespace,
				Expect(CreateNamespace(ctx, paulClient, chaniNamespace)).To(Succeed())

				// chani attempts to create chani's namespace, but crashes before returning
				failpoints.EnableFailPoint("panicKubeReadResp", 1)
				err := CreateNamespace(ctx, chaniClient, chaniNamespace)
				Expect(k8serrors.IsConflict(err) || k8serrors.IsAlreadyExists(err)).To(BeTrue())

				// Chani can't get her namespace - paul created it first and hasn't shared it
				Expect(k8serrors.IsNotFound(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())
			})

			It("recovers dual writes when there are spicedb write failures", func(ctx context.Context) {
				// paul creates his namespace
				Expect(CreateNamespace(ctx, paulClient, paulNamespace)).To(Succeed())

				// make spicedb write crash on chani's namespace write
				failpoints.EnableFailPoint("panicWriteSpiceDB", 1)
				Expect(CreateNamespace(ctx, chaniClient, chaniNamespace)).ToNot(BeNil())

				// paul creates chani's namespace so that the namespace exists
				Expect(CreateNamespace(ctx, paulClient, chaniNamespace)).To(Succeed())

				// check that chani can't get her namespace, indirectly showing
				// that the spicedb write was rolled back
				Expect(k8serrors.IsNotFound(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())

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

			It("recovers dual writes when spicedb write succeeds but crashes", func(ctx context.Context) {
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
				Expect(k8serrors.IsNotFound(GetNamespace(ctx, chaniClient, chaniNamespace))).To(BeTrue())

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
		}

		When("optimistic locking is used", func() {
			BeforeEach(func() {
				proxySrv.LockMode = distributedtx.StrategyOptimisticWriteToSpiceDBAndKube
			})
			AssertDualWriteBehavior()
		})

		When("pessimistic locking is used", func() {
			BeforeEach(func() {
				proxySrv.LockMode = distributedtx.StrategyPessimisticWriteToSpiceDBAndKube
			})
			AssertDualWriteBehavior()
		})
	})
})
