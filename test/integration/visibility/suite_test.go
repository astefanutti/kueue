/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package visibility

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/kueue/pkg/constants"
	"sigs.k8s.io/kueue/pkg/scheduler"

	config "sigs.k8s.io/kueue/apis/config/v1beta1"
	kueueclientset "sigs.k8s.io/kueue/client-go/clientset/versioned"
	visibilityv1alpha1 "sigs.k8s.io/kueue/client-go/clientset/versioned/typed/visibility/v1alpha1"
	"sigs.k8s.io/kueue/pkg/cache"
	"sigs.k8s.io/kueue/pkg/controller/core"
	"sigs.k8s.io/kueue/pkg/controller/core/indexer"
	"sigs.k8s.io/kueue/pkg/queue"
	"sigs.k8s.io/kueue/pkg/visibility"
	"sigs.k8s.io/kueue/pkg/webhooks"
	"sigs.k8s.io/kueue/test/integration/framework"
	// +kubebuilder:scaffold:imports
)

var (
	cfg              *rest.Config
	k8sClient        client.Client
	visibilityClient visibilityv1alpha1.VisibilityV1alpha1Interface
	fwk              *framework.Framework
	ctx              context.Context
)

func TestAPIs(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t,
		"Visibility Suite",
	)
}

var _ = ginkgo.BeforeSuite(func() {
	fwk = &framework.Framework{
		CRDPath:     filepath.Join("..", "..", "..", "config", "components", "crd", "bases"),
		WebhookPath: filepath.Join("..", "..", "..", "config", "components", "webhook"),
	}

	cfg = fwk.Init()
	serverCfg := visibility.CreateVisibilityServerConfig()

	kueueClient, err := kueueclientset.NewForConfig(serverCfg.LoopbackClientConfig)
	gomega.Expect(err).ToNot(gomega.HaveOccurred(), "unable to create kueue clientset")

	visibilityClient = kueueClient.VisibilityV1alpha1()

	ctx, k8sClient = fwk.RunManager(cfg, func(mgr manager.Manager, ctx context.Context) {
		err := indexer.Setup(ctx, mgr.GetFieldIndexer())
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		failedWebhook, err := webhooks.Setup(mgr)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "webhook", failedWebhook)

		cCache := cache.New(mgr.GetClient())
		queues := queue.NewManager(mgr.GetClient(), cCache)

		configuration := &config.Configuration{}
		mgr.GetScheme().Default(configuration)

		configuration.QueueVisibility = &config.QueueVisibility{
			UpdateIntervalSeconds: 2,
			ClusterQueues: &config.ClusterQueueVisibility{
				MaxCount: 3,
			},
		}

		failedCtrl, err := core.SetupControllers(mgr, queues, cCache, configuration)
		gomega.Expect(err).ToNot(gomega.HaveOccurred(), "controller", failedCtrl)

		sched := scheduler.New(queues, cCache, mgr.GetClient(), mgr.GetEventRecorderFor(constants.AdmissionName))
		err = sched.Start(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		go visibility.CreateAndStartVisibilityServerForConfig(serverCfg, queues, ctx)

		// wait for the extension API server to get started
		gomega.Eventually(func() error {
			result := visibilityClient.RESTClient().Get().Do(ctx)
			if result.Error() != nil {
				return err
			}
			return nil
		}).Should(gomega.Succeed())
	})
})

var _ = ginkgo.AfterSuite(func() {
	fwk.Teardown()
})
