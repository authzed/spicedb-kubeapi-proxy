//go:build e2e

package e2e

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

var _ = Describe("Proxy", func() {
	Describe("with a user", func() {
		var kwiznosClient kubernetes.Interface
		BeforeEach(func() {
			cfg := clientCA.GenerateUserConfig("kwiznos")
			restConfig, err := clientcmd.NewDefaultClientConfig(*cfg, nil).ClientConfig()
			Expect(err).To(Succeed())
			kwiznosClient, err = kubernetes.NewForConfig(restConfig)
			Expect(err).To(Succeed())
		})
		It("responds", func(ctx context.Context) {
			_, err := kwiznosClient.CoreV1().Namespaces().Get(ctx, "test", metav1.GetOptions{})

			// for now, just assert a 403 which indicates the client is connected
			// to the proxy and the proxy is connected to the apiserver.
			// TODO:
			//   - grant generated admin user admin access in the apisever
			//   - bootstrap a test schema that grants the generated kwiznos user access to a namespace
			//   - authorize non-kubectl responses
			Expect(k8serrors.IsUnexpectedServerError(err)).To(BeTrue())
		})
	})
})
