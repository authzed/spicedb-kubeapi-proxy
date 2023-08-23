//go:build e2e

package e2e

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var _ = Describe("Proxy", func() {
	Describe("with two users", func() {
		var paulClient, chaniClient kubernetes.Interface

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
		})

		It("doesn't show users namespaces the other has created", func(ctx context.Context) {
			const (
				paulNamespace  = "paul"
				chaniNamespace = "chani"
			)

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

			// neither can see each other's namespace
			paulVisibleNamespaces, err := paulClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			paulVisibleNamespaceNames := lo.Map(paulVisibleNamespaces.Items, func(item corev1.Namespace, index int) string {
				return item.Name
			})
			Expect(paulVisibleNamespaceNames).To(Equal([]string{"paul"}))

			chaniVisibleNamespaces, err := chaniClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
			Expect(err).To(Succeed())
			chaniVisibleNamespaceNames := lo.Map(chaniVisibleNamespaces.Items, func(item corev1.Namespace, index int) string {
				return item.Name
			})
			Expect(chaniVisibleNamespaceNames).To(Equal([]string{"chani"}))
		})
	})
})
