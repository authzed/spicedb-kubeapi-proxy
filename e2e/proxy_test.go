//go:build e2e

package e2e

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pingcap/failpoint"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var _ = Describe("Proxy", func() {
	Describe("with two users", func() {
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
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, paulNamespace, metav1.DeleteOptions{})
			_ = adminClient.CoreV1().Namespaces().Delete(ctx, chaniNamespace, metav1.DeleteOptions{})
		})

		It("doesn't show users namespaces the other has created", func(ctx context.Context) {
			// not created yet, neither can access
			_, err := paulClient.CoreV1().Namespaces().Get(ctx, paulNamespace, metav1.GetOptions{})
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())
			_, err = chaniClient.CoreV1().Namespaces().Get(ctx, chaniNamespace, metav1.GetOptions{})
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())

			// each creates their respective namespace
			_, err = paulClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: paulNamespace},
			}, metav1.CreateOptions{})
			Expect(err).To(Succeed())

			_, err = chaniClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: chaniNamespace},
			}, metav1.CreateOptions{})
			Expect(err).To(Succeed())

			// each can get their respective namespace
			_, err = paulClient.CoreV1().Namespaces().Get(ctx, paulNamespace, metav1.GetOptions{})
			Expect(err).To(Succeed())
			_, err = chaniClient.CoreV1().Namespaces().Get(ctx, chaniNamespace, metav1.GetOptions{})
			Expect(err).To(Succeed())

			// neither can get each other's namespace
			out, err := paulClient.CoreV1().Namespaces().Get(ctx, chaniNamespace, metav1.GetOptions{})
			fmt.Println(out)
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())
			_, err = chaniClient.CoreV1().Namespaces().Get(ctx, paulNamespace, metav1.GetOptions{})
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())

			// neither can see each other's namespace in the list
			paulVisibleNamespaces, err := paulClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			paulVisibleNamespaceNames := lo.Map(paulVisibleNamespaces.Items, func(item corev1.Namespace, index int) string {
				return item.Name
			})
			Expect(paulVisibleNamespaceNames).To(ContainElement(paulNamespace))

			chaniVisibleNamespaces, err := chaniClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			chaniVisibleNamespaceNames := lo.Map(chaniVisibleNamespaces.Items, func(item corev1.Namespace, index int) string {
				return item.Name
			})
			Expect(chaniVisibleNamespaceNames).To(ContainElement(chaniNamespace))
		})

		It("cleans up dual writes on errors", func(ctx context.Context) {
			// not created yet, neither can access
			_, err := paulClient.CoreV1().Namespaces().Get(ctx, paulNamespace, metav1.GetOptions{})
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())
			_, err = chaniClient.CoreV1().Namespaces().Get(ctx, chaniNamespace, metav1.GetOptions{})
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())

			// paul creates his namespace
			_, err = paulClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: paulNamespace},
			}, metav1.CreateOptions{})
			Expect(err).To(Succeed())

			// make kube write fail
			err = failpoint.Enable("github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy/panicKubeWrite", "panic")
			Expect(err).To(Succeed())

			_, err = chaniClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: chaniNamespace},
			}, metav1.CreateOptions{})
			Expect(err).ToNot(BeNil())

			// paul creates chani's namespace
			Expect(failpoint.Disable("github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy/panicKubeWrite")).To(Succeed())
			_, err = paulClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: chaniNamespace},
			}, metav1.CreateOptions{})
			Expect(err).To(Succeed())

			// paul can get get both namespaces
			_, err = paulClient.CoreV1().Namespaces().Get(ctx, paulNamespace, metav1.GetOptions{})
			Expect(err).To(Succeed())
			_, err = paulClient.CoreV1().Namespaces().Get(ctx, chaniNamespace, metav1.GetOptions{})
			Expect(err).To(Succeed())

			// chani can't get her namespace - this indicates the spicedb write was rolled back
			// from the failed dual write above
			_, err = chaniClient.CoreV1().Namespaces().Get(ctx, chaniNamespace, metav1.GetOptions{})
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())
		})

		It("recovers dual writes when it crashes", func(ctx context.Context) {
			// not created yet, neither can access
			_, err := paulClient.CoreV1().Namespaces().Get(ctx, paulNamespace, metav1.GetOptions{})
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())
			_, err = chaniClient.CoreV1().Namespaces().Get(ctx, chaniNamespace, metav1.GetOptions{})
			Expect(k8serrors.IsNotFound(err)).To(BeTrue())

			// paul creates his namespace
			_, err = paulClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: paulNamespace},
			}, metav1.CreateOptions{})
			Expect(err).To(Succeed())

			// make kube write succeed, but crash process before it can be recorded
			err = failpoint.Enable("github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy/panicKubeReadResp", "panic")
			Expect(err).To(Succeed())

			_, err = chaniClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: chaniNamespace},
			}, metav1.CreateOptions{})
			Expect(err).ToNot(BeNil())

			Expect(failpoint.Disable("github.com/authzed/spicedb-kubeapi-proxy/pkg/proxy/panicKubeReadResp")).To(Succeed())

			// Chani can get her namespace
			_, err = chaniClient.CoreV1().Namespaces().Get(ctx, chaniNamespace, metav1.GetOptions{})
			Expect(err).To(Succeed())
		})
	})
})
