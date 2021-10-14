package migration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// retainPolicy sets the PV's retainPolicy to "retain", so that when we remove the associated
// PVC, the PV does not get deleted with it.
func (s *state) retainPolicy(ctx context.Context, client kubernetesClient) (err error) {
	// we don't need to save the old policy at it is deleted in this process anyway.
	log.Debugf("%v/%v: changing retainPolicy for PV %v", s.ns.Name, s.sourcePVC.Name, s.sourcePV.Name)
	s.sourcePV, err = client.UpdatePV(ctx, s.sourcePV, func(pv *corev1.PersistentVolume) {
		pv.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
	})
	return errors.Wrapf(err, "could not update retainPolicy")
}

// scaleDown scales down all resources (hopefully) that are using the PVC. If there are
// daemonSets using it, it will return an error (we can't handle daemonSets). For jobs,
// it will just wait until they are completed.
//
// TODO: all these methods could probably be abstracted somehow, they have
// a lot of duplicated code...on the other hand, an abstraction may get too
// complicated because of the k8s clientset's structure.
func (s *state) scaleDown(ctx context.Context, client kubernetesClient) (err error) {
	sd := &scaleDown{s.ns.Name, ""}
	if s.sourcePVC != nil {
		sd.pvc = s.sourcePVC.Name
	} else {
		sd.pvc = removeMigrationSuffix(s.renamedPVC.Name)
	}
	log.Debugf("%v/%v: scaling down compute users of PVC", s.ns.Name, s.sourcePVC.Name)

	if err := sd.deployments(ctx, client.c); err != nil {
		return errors.Wrapf(err, "could not scale down Deployments")
	}

	if err := sd.statefulSets(ctx, client.c); err != nil {
		return errors.Wrapf(err, "could not scale down StatefulSets")
	}

	if err := sd.daemonSets(ctx, client.c); err != nil {
		return errors.Wrapf(err, "detected daemonsets and don't know what to do with them")
	}

	if err := sd.replicaSets(ctx, client.c); err != nil {
		return errors.Wrapf(err, "could not scale down ReplicaSets")
	}

	if err := sd.cronJobs(ctx, client.c); err != nil {
		return errors.Wrapf(err, "could not suspend CronJobs")
	}

	if err := sd.jobs(ctx, client.c); err != nil {
		return errors.Wrapf(err, "could not wait for Jobs to complete")
	}

	return nil
}

// recreatePVC removes the original PVC and creates a "renamed" one. It waits until the original
// PVC is removed.
func (s *state) recreatePVC(ctx context.Context, client kubernetesClient) (err error) {
	pvc := s.sourcePVC.DeepCopy()
	addAnnotation(pvc, isRenamedPVC, pvc.Name)
	pvc.Name = withMigrationSuffix(pvc.Name)
	pvc.ResourceVersion = ""

	s.renamedPVC, err = client.CreatePVC(ctx, pvc)
	if err != nil {
		return errors.Wrapf(err, "could not create renamed PVC")
	}

	pv, err := client.GetPV(ctx, s.sourcePVC.Spec.VolumeName)
	if err != nil {
		return errors.Wrapf(err, "could not check PV's retainPolicy")
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		return errors.Errorf("PV %v's retainPolicy is not set to 'retain'", pv.Name)
	}

	if err := client.DeletePVC(ctx, s.sourcePVC.Name, s.sourcePVC.Namespace); err != nil {
		return errors.Wrapf(err, "could not delete old PVC")
	}

	log.Debugf("%v/%v: waiting for old PVC to be gone", s.ns.Name, s.sourcePVC.Name)

	return waitUntil(func() error {
		_, err := client.GetPVC(ctx, s.sourcePVC.Name, s.sourcePVC.Namespace)
		if apierrors.IsNotFound(err) {
			s.sourcePVC = nil
			return nil
		}
		return errors.Errorf("PVC %v/%v still exists or other error %v", s.sourcePVC.Namespace, s.sourcePVC.Name, err)
	}, time.Second*1, time.Minute*2)
}

// waitUntil executes event every tickTime until it either returns nil or the
// timeoutTime has passed.
func waitUntil(event func() error, tickTime, timeoutTime time.Duration) error {
	if err := event(); err == nil {
		return nil
	}

	var err error
	tick := time.NewTicker(tickTime)
	timeout := time.NewTimer(timeoutTime)
	for {
		select {
		case <-tick.C:
		case <-timeout.C:
			return errors.Wrapf(err, "timeout")
		}

		err = event()
		if err == nil {
			return nil
		}
	}
}

// switchTargetPVC switches the PV's target PVC to the new renamed one. It waits for the bound to be
// successful on the renamed PVC's side.
func (s *state) switchTargetPVC(ctx context.Context, client kubernetesClient) (err error) {
	s.sourcePV, err = client.UpdatePV(ctx, s.sourcePV, func(pv *corev1.PersistentVolume) {
		// delete the PV's `claimRef`.
		pv.Spec.ClaimRef = nil
	})
	if err != nil {
		return errors.Wrapf(err, "could not remove claimRef from PV %v", (*s.sourcePV).Name)
	}

	// PV will be bound to the new (renamed) PVC, wait for this to happen.
	log.Debug("waiting for PV target to have switched")

	return waitUntil(func() error {
		s.renamedPVC, err = client.GetPVC(ctx, s.renamedPVC.Name, s.renamedPVC.Namespace)
		if err != nil {
			return errors.Wrapf(err, "could not get update for PVC %v", s.renamedPVC.Name)
		}
		if s.renamedPVC.Status.Phase == corev1.ClaimBound {
			log.Debugf("%v/%v: renamed PVC bound by old PV", s.ns.Name, s.renamedPVC.Name)
			return nil
		}
		return errors.Errorf("PVC phase not correct")
	}, time.Second*1, time.Minute*2)
}

// createTargetPVC creates the PVC. It also adds annotations so
// that ArgoCD will not try to prune / change it.
func (s *state) createTargetPVC(ctx context.Context, client kubernetesClient) (err error) {
	// copy the renamed PVC and adjust it so that we can bind a target Volume
	targetPVC := s.renamedPVC.DeepCopy()
	targetPVC.Name = removeMigrationSuffix(targetPVC.Name)
	targetPVC.Spec.StorageClassName = &globalTargetClass
	targetPVC.Spec.VolumeName = ""
	targetPVC.ResourceVersion = ""
	targetPVC.Status = corev1.PersistentVolumeClaimStatus{}
	// clean the annotations
	targetPVC.Annotations = make(map[string]string)

	addArgoAnnotations(targetPVC)

	s.targetPVC, err = client.CreatePVC(ctx, targetPVC)
	return errors.Wrapf(err, "could not create target PVC %v", targetPVC.Name)
}

// migrateData starts a migration Job that mounts both the renamed PVC and the new target
// PVC and rsync's the data over. Waits until this Job completes.
func (s *state) migrateData(ctx context.Context, client kubernetesClient) error {
	jobName := s.renamedPVC.Annotations[migrationJob]

	if jobName == "" {
		job, err := client.CreateJob(ctx, newJob(s.renamedPVC.Name, s.targetPVC.Name, s.ns.Name))
		if err != nil {
			return errors.Wrapf(err, "could not create migration job")
		}

		jobName = job.Name

		s.renamedPVC, err = client.UpdatePVC(ctx, s.renamedPVC, func(pvc *corev1.PersistentVolumeClaim) {
			addAnnotation(pvc, migrationJob, jobName)
		})
		if err != nil {
			return errors.Wrapf(err, "could not annotate PVC with jobName")
		}
	}

	return waitUntil(func() error {
		job, err := client.GetJob(ctx, jobName, s.ns.Name)
		if err != nil {
			return errors.Wrapf(err, "could not get update for Job %v", jobName)
		}

		if job.Status.Failed != 0 {
			return errors.New(fmt.Sprintf("migration job %v failed", job.Name))
		}
		if job.Status.Succeeded == 1 {
			return nil
		}
		return errors.Errorf("Job not finished yet")
	}, time.Second*1, time.Minute*30)
}

// scaleUp scales up resources again, after the migration.
func (s *state) scaleUp(ctx context.Context, client kubernetesClient) error {
	su := scaleUp{s.ns.Name, s.targetPVC, s.renamedPVC, ""}
	switch {
	case su.pvc != nil:
		su.pvcName = su.pvc.Name

	case su.renamedPVC != nil:
		su.pvcName = removeMigrationSuffix(su.renamedPVC.Name)

	default:
		return errors.New("cannot determine PVC name to scale up again")
	}

	if err := su.deployments(ctx, client.c); err != nil {
		return errors.Wrapf(err, "could not scale up Deployments")
	}

	if err := su.statefulSets(ctx, client.c); err != nil {
		return errors.Wrapf(err, "could not scale up StatefulSets")
	}

	if err := su.replicaSets(ctx, client.c); err != nil {
		return errors.Wrapf(err, "could not scale up ReplicaSets")
	}

	if err := su.cronJobs(ctx, client.c); err != nil {
		return errors.Wrapf(err, "could not suspend CronJobs")
	}

	return nil
}

// cleanUp removes the renamed PVC and the annotations that we don't need anymore.
func (s *state) cleanUp(ctx context.Context, client kubernetesClient) error {
	jobName := s.renamedPVC.Annotations[migrationJob]

	err := client.DeletePVC(ctx, s.renamedPVC.Name, s.renamedPVC.Namespace)
	if err != nil {
		return errors.Wrapf(err, "could not cleanup renamed PVC %v", s.renamedPVC.Name)
	}
	s.renamedPVC = nil

	pods, err := client.ListPods(ctx, s.targetPVC.Namespace)
	if err != nil {
		return errors.Wrapf(err, "could not list pods")
	}

	// delete all pods from our job (should only be one)
	for _, pod := range pods {
		for _, owner := range pod.OwnerReferences {
			if owner.APIVersion == "batch/v1" &&
				owner.Kind == "Job" &&
				owner.Name == jobName {
				err := client.DeletePod(ctx, pod.Name, pod.Namespace)
				if err != nil {
					return errors.Wrapf(err, "could not remove pod %v from job %v", pod.Name, jobName)
				}
			}
		}
	}

	err = client.DeleteJob(ctx, jobName, s.targetPVC.Namespace)
	if err != nil {
		return errors.Wrapf(err, "could not remove migration Job")
	}

	return nil
}

func withMigrationSuffix(s string) string {
	return s + migrationSuffix
}

func removeMigrationSuffix(s string) string {
	return strings.TrimSuffix(s, migrationSuffix)
}

func addArgoAnnotations(obj metav1.Object) {
	addAnnotation(obj, "argocd.argoproj.io/sync-options", "Prune=false")
	addAnnotation(obj, "argocd.argoproj.io/compare-options", "IgnoreExtraneous")
}

func newJob(sourcePVC, targetPVC, namespace string) *batchv1.Job {
	var compl int32 = 1
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "relokator-",
			Namespace:    namespace,
			Annotations: map[string]string{
				pvcMigration: targetPVC,
			},
		},
		Spec: batchv1.JobSpec{
			Completions:             &compl,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    "migrator",
							Image:   "eeacms/rsync",
							Command: []string{"/usr/bin/rsync"},
							Args:    []string{"-a", "/source/", "/target"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "source",
									ReadOnly:  true,
									MountPath: "/source",
								},
								{
									Name:      "target",
									MountPath: "/target",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "source",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: sourcePVC,
								},
							},
						},
						{
							Name: "target",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: targetPVC,
								},
							},
						},
					},
				},
			},
		},
	}
}
