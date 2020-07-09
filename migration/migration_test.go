package migration

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

type assertion func(testState, context.Context, kubernetesClient) error

func TestMigrate(t *testing.T) {
	ctx := context.Background()

	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("Could not start test environment: %v", err)
	}

	t.Cleanup(func() {
		if err := testEnv.Stop(); err != nil {
			t.Fatalf("could not stop test environment: %v", err)
		}
	})

	c, err := client.New(cfg, client.Options{})
	if err != nil {
		t.Fatalf("could not create client: %v", err)
	}

	cs := kubernetes.NewForConfigOrDie(cfg)

	if err = deployAll(ctx, globalResources, c); err != nil {
		t.Fatalf("error deploying global resources: %v", err)
	}

	assertions := []assertion{
		testState.assertRetainPolicy,
		testState.assertScaleDown,
		testState.assertRecreatePVC,
		testState.assertSwitchTargetPVC,
		testState.assertCreateTargetPVC,
		testState.assertMigrateData,
		testState.assertScaleUp,
		testState.assertCleanUp,
	}

	for _, test := range tests {
		test := test
		t.Run(test.namespace, func(t *testing.T) {
			t.Parallel()

			if err := deployAll(ctx, test.initial, c); err != nil {
				t.Fatalf("could not deploy initial resources: %v", err)
			}

			if err := test.preMigration(test.testState, ctx, kubernetesClient{cs}); err != nil {
				t.Fatalf("error executing preMigration steps: %v", err)
			}

			opts := append(test.options, AutoConfirmPrompts())
			m, err := New(ctx, cs, testOldStorageClass, testNewStorageClass, opts...)
			if err != nil {
				t.Fatalf("could not create instance of migrator: %v", err)
			}

			statusCheck, executedPhases := m.addAssertions(t, assertions, test.testState)
			defer close(statusCheck)

			if err := m.Migrate(); err != nil {
				t.Fatalf("Migration: %v", err)
			}

			// the last status check should not return an error either
			if err := <-statusCheck; err != nil {
				t.Errorf("error at last status check: %v", err)
			}

			if *executedPhases != test.expectedPhases {
				t.Errorf("incorrect amount of executed phases. expected=%v, got=%v", test.expectedPhases, *executedPhases)
			}

			if err := test.assertion(test.testState, ctx, kubernetesClient{cs}); err != nil {
				t.Errorf("postmigration assertion error: %v", err)
			}
		})
	}
}

// addAssertions adds the assertions to the phases (make sure that the given assertions actually match in order!)
// and adds statuschecks after every assertion.
// The assertions are run in parallel to the phase (in a separate subtest), the statuscheck runs after both
// have finished. The statusCheck happens async (because the phase wouldn't finish otherwise -> the status
// would never be updated -> the check would fail /timeout).
// The returned channel always returns exactly one value, the last error of the statusUpdate. The channel still
// needs to be closed afterwards!
func (m *Migrator) addAssertions(t *testing.T, assertions []assertion, ts testState) (lastStatusCheck chan error, executedPhases *int) {
	executedPhases = new(int)
	statusCheck := make(chan error)
	// the first status check needs to succeed in order for the first phase to start
	go func() { statusCheck <- nil }()

	// wrap all phases with our assertion function.
	for i := range m.phases {
		// capture range vars
		p := m.phases[i]
		assertion := assertions[i]

		// overwrite the original phase with the wrapped phase containing the assertion
		m.phases[i] = phase{
			string: p.string,
			exec: func(s *state, ctx context.Context, kc kubernetesClient) error {
				// first, ensure the last statusCheck is successful
				if err := <-statusCheck; err != nil {
					t.Fatalf("status check failed: %v", err)
				}

				var phaseErr error
				// run the assertion and the actual phase in parallel.
				t.Run(p.string, func(t *testing.T) {
					t.Run("assertion", func(t *testing.T) {
						t.Parallel()

						err := waitUntil(
							func() error { return assertion(ts, ctx, kc) },
							50*time.Millisecond, 10*time.Second,
						)
						if err != nil {
							t.Errorf("error on assertion: %v", err)
						}
					})

					t.Run("phase", func(t *testing.T) {
						t.Parallel()

						phaseErr = p.exec(s, ctx, kc)
						*executedPhases++
					})
				})

				// fire and forget about this. the next phase will wait for this
				// to return and check if the status is ok.
				go func() {
					statusCheck <- waitUntil(func() error {
						return ts.statusUpdated(ctx, kc, p.string)
					}, 50*time.Millisecond, 5*time.Second)
				}()

				return phaseErr
			},
		}
	}
	return statusCheck, executedPhases
}

// assertRetainPolicy ensures that the retain policy is set to retain.
func (ts testState) assertRetainPolicy(ctx context.Context, client kubernetesClient) error {
	pv, err := client.GetPV(ctx, ts.pv)
	if err != nil {
		return err
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != core.PersistentVolumeReclaimRetain {
		return errors.Errorf("retain policy not correct. expected=%s, got=%s",
			core.PersistentVolumeReclaimRetain, pv.Spec.PersistentVolumeReclaimPolicy)
	}
	return nil
}

// assertScaleDown ensures that the deployment is scaled down to 0 replicas.
func (ts testState) assertScaleDown(ctx context.Context, client kubernetesClient) error {
	d, err := client.c.AppsV1().Deployments(ts.namespace).Get(ctx, ts.deployment, meta.GetOptions{})
	if err != nil {
		return err
	}

	if d.Spec.Replicas != nil && *d.Spec.Replicas == 0 {
		return nil
	}
	return errors.Errorf("replicas not yet correct. expected=%v, got=%v", 0, *d.Spec.Replicas)
}

// assertRecreatePVC ensures that the PVC with the migration-suffix (the "renamed" PVC) is
// created, has the correct annotations and makes the old / original PVC "deletable" by
// removing the PVC protection finalizer.
func (ts testState) assertRecreatePVC(ctx context.Context, client kubernetesClient) error {
	pvc, err := client.GetPVC(ctx, withMigrationSuffix(ts.pvc), ts.namespace)
	if err != nil {
		return err
	}

	if ann, ok := pvc.Annotations[isRenamedPVC]; !ok || ann != ts.pvc {
		return errors.Errorf("annotation does not exist or is wrong: %v", pvc.Annotations[isRenamedPVC])
	}

	return makeDeletable(ctx, ts.pvc, ts.namespace, client)
}

// makeDeletable removes the finalizers from a PVC if it should still exist.
func makeDeletable(ctx context.Context, pvcName, namespace string, client kubernetesClient) error {
	pvc, err := client.GetPVC(ctx, pvcName, namespace)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "could not get PVC")
	}
	if !apierrors.IsNotFound(err) {
		_, err = client.UpdatePVC(ctx, pvc, func(pvc *core.PersistentVolumeClaim) {
			// remove the protection finalizer
			pvc.Finalizers = nil
		})
		if err == nil {
			return errors.Errorf("pvc still exists: %v", pvc.Finalizers)
		}
		if !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "expected not found, got")
		}
	}
	return nil
}

// assertSwitchTargetPVC sets the claimRef of the PV to the renamed PVC and updates the renamed
// PVC's status.
func (ts testState) assertSwitchTargetPVC(ctx context.Context, client kubernetesClient) error {
	pv, err := client.GetPV(ctx, ts.pv)
	if err != nil {
		return err
	}
	if pv.Spec.ClaimRef != nil {
		return errors.Errorf("PV still bound")
	}

	_, err = client.UpdatePV(ctx, pv, func(pv *core.PersistentVolume) {
		pv.Spec.ClaimRef = &core.ObjectReference{
			Namespace: ts.namespace,
			Name:      withMigrationSuffix(ts.pvc),
		}
	})
	if err != nil {
		return err
	}

	pvc, err := client.GetPVC(ctx, withMigrationSuffix(ts.pvc), ts.namespace)
	if err != nil {
		return err
	}
	pvc.Status.Phase = core.ClaimBound

	_, err = client.c.CoreV1().PersistentVolumeClaims(ts.namespace).UpdateStatus(ctx, pvc, meta.UpdateOptions{})
	return err
}

// assertCreateTargetPVC ensures that the new PVC is created, has the correct storage class and
// tthe argo annotations set.
func (ts testState) assertCreateTargetPVC(ctx context.Context, client kubernetesClient) error {
	pvc, err := client.GetPVC(ctx, ts.pvc, ts.namespace)
	if err != nil {
		return err
	}

	if pvc.Annotations["argocd.argoproj.io/sync-options"] != "Prune=false" {
		return errors.Errorf("PVC does not have Argo Annotations")
	}
	if pvc.Spec.StorageClassName == nil {
		return errors.Errorf("New PVC's storage Class is not set")
	}

	if *pvc.Spec.StorageClassName != testNewStorageClass {
		return errors.Errorf("New PVC's storage Class is incorrect: %v", *pvc.Spec.StorageClassName)
	}

	return nil
}

// assertMigrateData ensures that a job is created, creates a dummy pod with that job as
// an owner, sets the pod's status to succeeded and the job's status to succeeded too.
func (ts testState) assertMigrateData(ctx context.Context, client kubernetesClient) error {
	jobs, err := client.c.BatchV1().Jobs(ts.namespace).List(ctx, meta.ListOptions{})
	if err != nil {
		return err
	}

	var job *batch.Job
	for _, j := range jobs.Items {
		if j.Annotations[pvcMigration] == ts.pvc {
			job = &j
			break
		}
	}
	if job == nil {
		return errors.Errorf("no job for PVC %v found", ts.pvc)
	}

	specTmpl := newJob(ts.pvc, withMigrationSuffix(ts.pvc), ts.namespace).Spec.Template.Spec

	// create a pod. This pod is expected to be removed at the cleanup stage.
	pod := &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			GenerateName: "migration-job",
			Namespace:    ts.namespace,
			OwnerReferences: []meta.OwnerReference{
				{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       job.Name,
					UID:        job.UID,
				},
			},
		},
		Spec: specTmpl,
	}

	pod, err = client.c.CoreV1().Pods(ts.namespace).Create(ctx, pod, meta.CreateOptions{})
	if err != nil {
		return err
	}

	pod.Status.Phase = core.PodSucceeded
	_, err = client.c.CoreV1().Pods(ts.namespace).UpdateStatus(ctx, pod, meta.UpdateOptions{})
	if err != nil {
		return err
	}

	pvc, err := client.GetPVC(ctx, withMigrationSuffix(ts.pvc), ts.namespace)
	if err != nil {
		return err
	}

	if pvc.Annotations[migrationJob] != job.Name {
		return errors.Errorf("PVC does not have annotation for migration job")
	}

	job.Status.Succeeded = 1

	_, err = client.c.BatchV1().Jobs(ts.namespace).UpdateStatus(ctx, job, meta.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

// assertScaleUp ensures that the deployment has the right number of replicas again.
func (ts testState) assertScaleUp(ctx context.Context, client kubernetesClient) error {
	deploy, err := client.c.AppsV1().Deployments(ts.namespace).Get(ctx, ts.deployment, meta.GetOptions{})
	if err != nil {
		return err
	}

	if deploy.Spec.Replicas == nil {
		return errors.Errorf("deployment does not have any replicas set")
	}

	if *deploy.Spec.Replicas != ts.deploymentReplicas {
		return errors.Errorf("deployment has wrong number of replicas. expected=%v, got=%v",
			ts.deploymentReplicas, *deploy.Spec.Replicas)
	}

	return nil
}

// assertCleanUp ensures that there are no more migration-pods lying around anymore.
func (ts testState) assertCleanUp(ctx context.Context, client kubernetesClient) error {
	err := makeDeletable(ctx, withMigrationSuffix(ts.pvc), ts.namespace, client)
	if err != nil {
		return err
	}

	pods, err := client.ListPods(ctx, ts.namespace)
	if err != nil {
		return err
	}
	if len(pods) != 0 {
		return errors.Errorf("cleanup unsuccessful, still %v pods", len(pods))
	}
	return nil
}

// statusUpdated checks if the PVC, if it exists, has been updated to the specified phase. If not,
// it will return an error.
func (ts testState) statusUpdated(ctx context.Context, client kubernetesClient, phase string) error {
	pvcs := []string{ts.pvc, withMigrationSuffix(ts.pvc)}
	for _, pvcName := range pvcs {
		pvc, err := client.GetPVC(ctx, pvcName, ts.namespace)
		if err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		if apierrors.IsNotFound(err) {
			continue
		}

		if pvc.Annotations[completedMigrationPhase] != phase {
			return errors.Errorf("phase incorrect. expected=%v, got=%v",
				phase, pvc.Annotations[completedMigrationPhase])
		}

	}
	return nil
}

func deployAll(ctx context.Context, objects []runtime.Object, c client.Client) error {
	for _, obj := range objects {
		if err := c.Create(ctx, obj); err != nil {
			return err
		}
	}
	return nil
}

func newInt(i int32) *int32 {
	return &i
}
