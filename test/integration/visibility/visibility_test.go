/*
Copyright 2024 The Kubernetes Authors.

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
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta1"
	visibility "sigs.k8s.io/kueue/apis/visibility/v1alpha1"
	"sigs.k8s.io/kueue/pkg/util/testing"
	"sigs.k8s.io/kueue/test/util"
)

var pendingWorkloadCmpOpts = []cmp.Option{
	cmpopts.IgnoreFields(metav1.ObjectMeta{}, "Name"),
	cmpopts.IgnoreFields(metav1.ObjectMeta{}, "CreationTimestamp"),
	cmpopts.IgnoreTypes([]metav1.OwnerReference{}),
}

var _ = Describe("Kueue visibility API", Ordered, func() {
	const defaultFlavor = "default-flavor"

	var (
		resourceFlavor *kueue.ResourceFlavor
		clusterQueue   *kueue.ClusterQueue
		localQueue     *kueue.LocalQueue
		ns             *corev1.Namespace
	)

	BeforeAll(func() {
		resourceFlavor = testing.MakeResourceFlavor(defaultFlavor).Obj()
		Expect(k8sClient.Create(ctx, resourceFlavor)).To(Succeed())

		clusterQueue = testing.MakeClusterQueue("cluster-queue").
			ResourceGroup(
				*testing.MakeFlavorQuotas(defaultFlavor).
					Resource(corev1.ResourceCPU, "1").
					Obj(),
			).
			Obj()
		Expect(k8sClient.Create(ctx, clusterQueue)).To(Succeed())
	})

	AfterAll(func() {
		util.ExpectClusterQueueToBeDeleted(ctx, k8sClient, clusterQueue, true)
		util.ExpectResourceFlavorToBeDeleted(ctx, k8sClient, resourceFlavor, true)
	})

	BeforeEach(func() {
		ns = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "e2e-",
			},
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		localQueue = testing.MakeLocalQueue("queue", ns.Name).ClusterQueue(clusterQueue.Name).Obj()
		Expect(k8sClient.Create(ctx, localQueue)).To(Succeed())
	})

	AfterEach(func() {
		Expect(util.DeleteNamespace(ctx, k8sClient, ns)).To(Succeed())
	})

	When("There are no workloads in the queue", func() {
		It("Should report no pending workloads", func() {
			By("Calling the visibility API for the cluster queue", func() {
				summary, err := visibilityClient.
					ClusterQueues().
					GetPendingWorkloadsSummary(ctx, clusterQueue.Name, metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred())
				Expect(summary.Items).To(HaveLen(0))
			})

			By("Calling the visibility API for the local queue", func() {
				summary, err := visibilityClient.
					LocalQueues(ns.Name).
					GetPendingWorkloadsSummary(ctx, localQueue.Name, metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred())
				Expect(summary.Items).To(HaveLen(0))
			})
		})
	})

	When("There isn't any more quota available", func() {
		var admittedWL *kueue.Workload
		BeforeEach(func() {
			admittedWL = testing.MakeWorkload("wl1", ns.Name).
				Queue(localQueue.Name).
				Request(corev1.ResourceCPU, "1").
				Obj()
			Expect(k8sClient.Create(ctx, admittedWL)).To(Succeed())

			Eventually(GetObject(admittedWL), util.Timeout, util.Interval).
				Should(WithTransform(conditions, Not(BeEmpty())))
		})

		It("Should report no pending workloads", func() {
			By("Calling the visibility API for the cluster queue", func() {
				summary, err := visibilityClient.
					ClusterQueues().
					GetPendingWorkloadsSummary(ctx, clusterQueue.Name, metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred())
				Expect(summary.Items).To(HaveLen(0))
			})

			By("Calling the visibility API for the local queue", func() {
				summary, err := visibilityClient.
					LocalQueues(ns.Name).
					GetPendingWorkloadsSummary(ctx, localQueue.Name, metav1.GetOptions{})
				Expect(err).NotTo(HaveOccurred())
				Expect(summary.Items).To(HaveLen(0))
			})
		})

		It("Should report a pending workload", func() {
			var wl *kueue.Workload
			By("Creating a second workload", func() {
				wl = testing.MakeWorkload("wl2", ns.Name).
					Queue(localQueue.Name).
					Request(corev1.ResourceCPU, "1").
					Obj()
				Expect(k8sClient.Create(ctx, wl)).To(Succeed())

				Eventually(GetObject(wl), util.Timeout, util.Interval).
					Should(WithTransform(conditions, Not(BeEmpty())))
			})

			By("Calling the visibility API for the cluster queue", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						ClusterQueues().
						GetPendingWorkloadsSummary(ctx, clusterQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).
					Should(HaveExactElements(
						BeComparableTo(visibility.PendingWorkload{
							ObjectMeta: metav1.ObjectMeta{
								Namespace: ns.Name,
								Name:      wl.Name,
							},
							PositionInLocalQueue:   0,
							PositionInClusterQueue: 0,
							LocalQueueName:         localQueue.Name,
						}, pendingWorkloadCmpOpts...)))
			})

			By("Calling the visibility API for the local queue", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						LocalQueues(ns.Name).
						GetPendingWorkloadsSummary(ctx, localQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).
					Should(HaveExactElements(
						BeComparableTo(visibility.PendingWorkload{
							ObjectMeta: metav1.ObjectMeta{
								Namespace: ns.Name,
								Name:      wl.Name,
							},
							PositionInLocalQueue:   0,
							PositionInClusterQueue: 0,
							LocalQueueName:         localQueue.Name,
						}, pendingWorkloadCmpOpts...)))
			})
		})

		It("Should not report any pending workloads after the pending workload is deleted", func() {
			var wl *kueue.Workload
			By("Creating a second workload", func() {
				wl = testing.MakeWorkload("wl2", ns.Name).
					Queue(localQueue.Name).
					Request(corev1.ResourceCPU, "1").
					Obj()
				Expect(k8sClient.Create(ctx, wl)).To(Succeed())

				Eventually(GetObject(wl), util.Timeout, util.Interval).
					Should(WithTransform(conditions, Not(BeEmpty())))
			})

			By("Verifying the visibility API reports the pending workload", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						ClusterQueues().
						GetPendingWorkloadsSummary(ctx, clusterQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).
					Should(HaveExactElements(
						BeComparableTo(visibility.PendingWorkload{
							ObjectMeta: metav1.ObjectMeta{
								Namespace: ns.Name,
								Name:      wl.Name,
							},
							PositionInLocalQueue:   0,
							PositionInClusterQueue: 0,
							LocalQueueName:         localQueue.Name,
						}, pendingWorkloadCmpOpts...)))
			})

			By("Deleting the second workload", func() {
				Expect(k8sClient.Delete(ctx, wl)).To(Succeed())
				Eventually(ErrorFrom(GetObject(wl))).
					Should(MatchError(errors.NewNotFound(kueue.Resource("workloads"), wl.Name)))
			})

			By("Calling the visibility API for the cluster queue", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						ClusterQueues().
						GetPendingWorkloadsSummary(ctx, clusterQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).Should(HaveLen(0))
			})

			By("Calling the visibility API for the local queue", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						LocalQueues(ns.Name).
						GetPendingWorkloadsSummary(ctx, localQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).Should(HaveLen(0))
			})
		})

		It("Should not report any pending workloads after the admitted workload is deleted", func() {
			var wl *kueue.Workload
			By("Creating a second workload", func() {
				wl = testing.MakeWorkload("wl2", ns.Name).
					Queue(localQueue.Name).
					Request(corev1.ResourceCPU, "1").
					Obj()
				Expect(k8sClient.Create(ctx, wl)).To(Succeed())

				Eventually(GetObject(wl), util.Timeout, util.Interval).
					Should(WithTransform(conditions, Not(BeEmpty())))
			})

			By("Verifying the visibility API reports the pending workload", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						ClusterQueues().
						GetPendingWorkloadsSummary(ctx, clusterQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).
					Should(HaveExactElements(
						BeComparableTo(visibility.PendingWorkload{
							ObjectMeta: metav1.ObjectMeta{
								Namespace: ns.Name,
								Name:      wl.Name,
							},
							PositionInLocalQueue:   0,
							PositionInClusterQueue: 0,
							LocalQueueName:         localQueue.Name,
						}, pendingWorkloadCmpOpts...)))
			})

			By("Deleting the admitted workload", func() {
				Expect(k8sClient.Delete(ctx, admittedWL)).To(Succeed())
				Eventually(ErrorFrom(GetObject(admittedWL))).
					Should(MatchError(errors.NewNotFound(kueue.Resource("workloads"), admittedWL.Name)))
			})

			By("Calling the visibility API for the cluster queue", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						ClusterQueues().
						GetPendingWorkloadsSummary(ctx, clusterQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).Should(HaveLen(0))
			})

			By("Calling the visibility API for the local queue", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						LocalQueues(ns.Name).
						GetPendingWorkloadsSummary(ctx, localQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).Should(HaveLen(0))
			})
		})

		It("Should report pending workloads according to their priorities", func() {
			var (
				wl2 *kueue.Workload
				wl3 *kueue.Workload
				wl4 *kueue.Workload
			)
			By("Creating three workloads with different workload priorities", func() {
				wl2 = testing.MakeWorkload("wl2", ns.Name).
					Queue(localQueue.Name).
					Request(corev1.ResourceCPU, "1").
					Priority(1).
					Obj()
				Expect(k8sClient.Create(ctx, wl2)).To(Succeed())

				Eventually(GetObject(wl2), util.Timeout, util.Interval).
					Should(WithTransform(conditions, Not(BeEmpty())))

				wl3 = testing.MakeWorkload("wl3", ns.Name).
					Queue(localQueue.Name).
					Request(corev1.ResourceCPU, "1").
					Priority(2).
					Obj()
				Expect(k8sClient.Create(ctx, wl3)).To(Succeed())

				Eventually(GetObject(wl3), util.Timeout, util.Interval).
					Should(WithTransform(conditions, Not(BeEmpty())))

				wl4 = testing.MakeWorkload("wl4", ns.Name).
					Queue(localQueue.Name).
					Request(corev1.ResourceCPU, "1").
					Priority(3).
					Obj()
				Expect(k8sClient.Create(ctx, wl4)).To(Succeed())

				Eventually(GetObject(wl4), util.Timeout, util.Interval).
					Should(WithTransform(conditions, Not(BeEmpty())))
			})

			By("Verifying the visibility API reports the correct positions and priorities", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						ClusterQueues().
						GetPendingWorkloadsSummary(ctx, clusterQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).
					Should(HaveExactElements(
						BeComparableTo(visibility.PendingWorkload{
							ObjectMeta: metav1.ObjectMeta{
								Namespace: ns.Name,
								Name:      wl4.Name,
							},
							PositionInLocalQueue:   0,
							PositionInClusterQueue: 0,
							Priority:               3,
							LocalQueueName:         localQueue.Name,
						}, pendingWorkloadCmpOpts...),
						BeComparableTo(visibility.PendingWorkload{
							ObjectMeta: metav1.ObjectMeta{
								Namespace: ns.Name,
								Name:      wl3.Name,
							},
							PositionInLocalQueue:   1,
							PositionInClusterQueue: 1,
							Priority:               2,
							LocalQueueName:         localQueue.Name,
						}, pendingWorkloadCmpOpts...),
						BeComparableTo(visibility.PendingWorkload{
							ObjectMeta: metav1.ObjectMeta{
								Namespace: ns.Name,
								Name:      wl2.Name,
							},
							PositionInLocalQueue:   2,
							PositionInClusterQueue: 2,
							Priority:               1,
							LocalQueueName:         localQueue.Name,
						}, pendingWorkloadCmpOpts...),
					))
			})

			By("Deleting a pending workload", func() {
				Expect(k8sClient.Delete(ctx, wl3)).To(Succeed())
				Eventually(ErrorFrom(GetObject(wl3))).
					Should(MatchError(errors.NewNotFound(kueue.Resource("workloads"), wl3.Name)))
			})

			By("Verifying the visibility API reports the correct positions and priorities", func() {
				Eventually(func() []visibility.PendingWorkload {
					summary, err := visibilityClient.
						ClusterQueues().
						GetPendingWorkloadsSummary(ctx, clusterQueue.Name, metav1.GetOptions{})
					Expect(err).NotTo(HaveOccurred())
					return summary.Items
				}, util.Timeout, util.Interval).
					Should(HaveExactElements(
						BeComparableTo(visibility.PendingWorkload{
							ObjectMeta: metav1.ObjectMeta{
								Namespace: ns.Name,
								Name:      wl4.Name,
							},
							PositionInLocalQueue:   0,
							PositionInClusterQueue: 0,
							Priority:               3,
							LocalQueueName:         localQueue.Name,
						}, pendingWorkloadCmpOpts...),
						BeComparableTo(visibility.PendingWorkload{
							ObjectMeta: metav1.ObjectMeta{
								Namespace: ns.Name,
								Name:      wl2.Name,
							},
							PositionInLocalQueue:   1,
							PositionInClusterQueue: 1,
							Priority:               1,
							LocalQueueName:         localQueue.Name,
						}, pendingWorkloadCmpOpts...),
					))
			})
		})
	})
})

func GetObject(obj client.Object) func(g Gomega) (client.Object, error) {
	return func(g Gomega) (client.Object, error) {
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), obj)
		return obj, err
	}
}

func ErrorFrom[T any](fn func(g Gomega) (T, error)) func(g Gomega) error {
	return func(g Gomega) error {
		_, err := fn(g)
		return err
	}
}

func conditions(wl *kueue.Workload) []metav1.Condition {
	return wl.Status.Conditions
}
