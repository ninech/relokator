package migration

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type scaleDown struct {
	namespace string
	pvc       string
}

type scaleUp struct {
	namespace  string
	pvc        *corev1.PersistentVolumeClaim
	renamedPVC *corev1.PersistentVolumeClaim
	pvcName    string
}

func (s scaleDown) deployments(ctx context.Context, client kubernetes.Interface) error {
	deployments, err := client.AppsV1().Deployments(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	for _, deploy := range deployments.Items {
		for _, vol := range deploy.Spec.Template.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			if vol.PersistentVolumeClaim.ClaimName != s.pvc {
				continue
			}
			// ensure that we have not processed it before already
			if _, ok := deploy.Annotations[pvcMigration]; ok {
				continue
			}

			// has PVC, scale down
			if deploy.Spec.Replicas == nil {
				addAnnotation(&deploy, oldReplicaCount, "-1")
			} else {
				addAnnotation(&deploy, oldReplicaCount, strconv.Itoa(int(*deploy.Spec.Replicas)))
			}
			addAnnotation(&deploy, pvcMigration, s.pvc)
			deploy.Spec.Replicas = new(int32) // default 0

			_, err := client.AppsV1().Deployments(s.namespace).Update(ctx, &deploy, metav1.UpdateOptions{})
			if err != nil {
				return errors.Wrapf(err, "could not update deployment %v", deploy.Name)
			}
			log.Debugf("%v/%v: scaled down deployment %v", s.namespace, s.pvc, deploy.Name)

			break
		}
	}
	return nil
}

func (s scaleUp) deployments(ctx context.Context, client kubernetes.Interface) error {
	deployments, err := client.AppsV1().Deployments(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}

	for _, deploy := range deployments.Items {
		if name, ok := deploy.Annotations[pvcMigration]; !ok || name != s.pvcName {
			continue
		}

		oldCount, ok := deploy.Annotations[oldReplicaCount]
		if !ok {
			// shouldn't happen, so we return an error
			return errors.New(fmt.Sprintf(
				"deployment %v is part of this migration, but does not have an old replicaCount",
				deploy.Name))
		}

		c, err := strconv.Atoi(oldCount)
		if err != nil {
			return errors.Wrapf(err, "could not determine old number of replicas for deployment %v", deploy.Name)
		}

		c32 := int32(c)
		deploy.Spec.Replicas = &c32
		replicas := strconv.Itoa(c)
		if c == -1 {
			replicas = "undefined"
			deploy.Spec.Replicas = nil
		}

		delete(deploy.Annotations, oldReplicaCount)
		delete(deploy.Annotations, pvcMigration)

		if _, err := client.AppsV1().Deployments(deploy.Namespace).Update(ctx, &deploy, metav1.UpdateOptions{}); err != nil {
			return errors.Wrapf(err, "could not scale up deployment %v", deploy.Name)
		}
		log.Debugf("%v/%v: scaled up deployment %v to %v replicas", s.namespace, s.pvc.Name, deploy.Name, replicas)
	}
	return nil
}

func (s scaleDown) statefulSets(ctx context.Context, client kubernetes.Interface) error {
	sets, err := client.AppsV1().StatefulSets(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, set := range sets.Items {
		if !s.shouldScale(set) {
			continue
		}

		// has PVC, scale down
		if set.Spec.Replicas == nil {
			addAnnotation(&set, oldReplicaCount, "-1")
		} else {
			addAnnotation(&set, oldReplicaCount, strconv.Itoa(int(*set.Spec.Replicas)))
		}
		addAnnotation(&set, pvcMigration, s.pvc)
		set.Spec.Replicas = new(int32) // default 0

		_, err := client.AppsV1().StatefulSets(s.namespace).Update(ctx, &set, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "could not update statefulSet %v", set.Name)
		}

		log.Debugf("%v/%v: scaled down statefulSet %v", s.namespace, s.pvc, set.Name)
	}
	return nil
}

func (s scaleDown) shouldScale(set appsv1.StatefulSet) bool {
	// ensure that we have not processed it before already
	if _, ok := set.Annotations[pvcMigration]; ok {
		return false
	}

	for _, vol := range set.Spec.VolumeClaimTemplates {
		// volumes created by a volumeClaimTemplate are named volName-stsName-num.
		// in order to find out if our pvc has been created by this sts,
		// we use this regexp.
		match, err := regexp.MatchString(fmt.Sprintf("(%s-%s-)[0-9]+", vol.ObjectMeta.Name, set.Name), s.pvc)
		if err != nil {
			return false
		}

		if match {
			return true
		}
	}

	for _, vol := range set.Spec.Template.Spec.Volumes {
		if vol.PersistentVolumeClaim == nil {
			continue
		}

		if vol.PersistentVolumeClaim.ClaimName == s.pvc {
			return true
		}
	}

	return false
}

func (s scaleUp) statefulSets(ctx context.Context, client kubernetes.Interface) error {
	sets, err := client.AppsV1().StatefulSets(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}

	for _, set := range sets.Items {
		if name, ok := set.Annotations[pvcMigration]; !ok || name != s.pvcName {
			continue
		}

		oldCount, ok := set.Annotations[oldReplicaCount]
		if !ok {
			// shouldn't happen, so we return an error
			return errors.New(fmt.Sprintf(
				"statefulSet %v is part of this migration, but does not have an old replicaCount",
				set.Name))
		}

		c, err := strconv.Atoi(oldCount)
		if err != nil {
			return errors.Wrapf(err, "could not determine old number of replicas for deployment %v", set.Name)
		}

		c32 := int32(c)
		set.Spec.Replicas = &c32
		replicas := strconv.Itoa(c)
		if c == -1 {
			set.Spec.Replicas = nil
			replicas = "undefined"
		}
		delete(set.Annotations, oldReplicaCount)
		delete(set.Annotations, pvcMigration)

		if _, err := client.AppsV1().StatefulSets(set.Namespace).Update(ctx, &set, metav1.UpdateOptions{}); err != nil {
			return errors.Wrapf(err, "could not scale up statefulset %v", set.Name)
		}
		log.Debugf("%v/%v: scaled up statefulset %v to %v replicas", s.namespace, s.pvc.Name, set.Name, replicas)
	}
	return nil
}

func hasControllingOwner(or []metav1.OwnerReference) bool {
	if len(or) == 0 {
		return false
	}
	for _, own := range or {
		if own.Controller != nil && *own.Controller {
			return true
		}
	}
	return false
}

func (s scaleDown) replicaSets(ctx context.Context, client kubernetes.Interface) error {
	sets, err := client.AppsV1().ReplicaSets(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	for _, set := range sets.Items {
		// only process replicaSets that have no controlling owner
		if hasControllingOwner(set.OwnerReferences) {
			continue
		}

		for _, vol := range set.Spec.Template.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			if vol.PersistentVolumeClaim.ClaimName != s.pvc {
				continue
			}

			// ensure that we have not processed it before already
			if _, ok := set.Annotations[pvcMigration]; ok {
				continue
			}

			// has PVC, scale down
			if set.Spec.Replicas == nil {
				addAnnotation(&set, oldReplicaCount, "-1")
			} else {
				addAnnotation(&set, oldReplicaCount, strconv.Itoa(int(*set.Spec.Replicas)))
			}
			addAnnotation(&set, pvcMigration, s.pvc)
			set.Spec.Replicas = new(int32) // default 0

			_, err := client.AppsV1().ReplicaSets(s.namespace).Update(ctx, &set, metav1.UpdateOptions{})
			if err != nil {
				return errors.Wrapf(err, "could not update deployment %v", set.Name)
			}

			log.Debugf("%v/%v: scaled down replicaSet %v", s.namespace, s.pvc, set.Name)

			break
		}
	}
	return nil
}
func (s scaleUp) replicaSets(ctx context.Context, client kubernetes.Interface) error {
	sets, err := client.AppsV1().ReplicaSets(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}

	for _, set := range sets.Items {
		// only process replicaSets that have no controlling owner
		if hasControllingOwner(set.OwnerReferences) {
			continue
		}

		if name, ok := set.Annotations[pvcMigration]; !ok || name != s.pvcName {
			continue
		}

		oldCount, ok := set.Annotations[oldReplicaCount]
		if !ok {
			// shouldn't happen, so we return an error
			return errors.New(fmt.Sprintf(
				"statefulSet %v is part of this migration, but does not have an old replicaCount",
				set.Name))
		}

		c, err := strconv.Atoi(oldCount)
		if err != nil {
			return errors.Wrapf(err, "could not determine old number of replicas for deployment %v", set.Name)
		}

		c32 := int32(c)
		set.Spec.Replicas = &c32
		replicas := strconv.Itoa(c)
		if c == -1 {
			replicas = "undefined"
			set.Spec.Replicas = nil
		}
		delete(set.Annotations, oldReplicaCount)
		delete(set.Annotations, pvcMigration)

		if _, err := client.AppsV1().ReplicaSets(set.Namespace).Update(ctx, &set, metav1.UpdateOptions{}); err != nil {
			return errors.Wrapf(err, "could not scale up statefulset %v", set.Name)
		}
		log.Debugf("%v/%v: scaled up replicaSet %v to %v replicas", s.namespace, s.pvc, set.Name, replicas)
	}
	return nil
}

func (s scaleDown) daemonSets(ctx context.Context, client kubernetes.Interface) error {
	sets, err := client.AppsV1().DaemonSets(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	for _, set := range sets.Items {
		// only process replicaSets that have no owner
		if len(set.OwnerReferences) != 0 {
			continue
		}

		for _, vol := range set.Spec.Template.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			if vol.PersistentVolumeClaim.ClaimName != s.pvc {
				continue
			}

			return errors.New(fmt.Sprintf("cannot process daemonSet %v, you need to scale them down manually", set.Name))
		}
	}
	return nil
}

func (s scaleDown) cronJobs(ctx context.Context, client kubernetes.Interface) error {
	jobs, err := client.BatchV1beta1().CronJobs(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	for _, job := range jobs.Items {
		for _, vol := range job.Spec.JobTemplate.Spec.Template.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			if vol.PersistentVolumeClaim.ClaimName != s.pvc {
				continue
			}

			// if it already is suspended, process the next cronJob
			if job.Spec.Suspend != nil && *job.Spec.Suspend {
				break
			}

			suspend := true
			job.Spec.Suspend = &suspend
			// just set something that we now we changed it
			addAnnotation(&job, pvcMigration, s.pvc)

			_, err := client.BatchV1beta1().CronJobs(s.namespace).Update(ctx, &job, metav1.UpdateOptions{})
			if err != nil {
				return errors.Wrapf(err, "could not suspend cronJob %v", job.Name)
			}

			c := &job

			if len(c.Status.Active) == 0 {
				break
			}

			// wait for the cronJob to finish
			tick := time.NewTicker(time.Second * 5)
			timeout := time.NewTimer(time.Minute * 2)
			for {
				select {
				case <-timeout.C:
					return errors.New(fmt.Sprintf("timeout waiting for cronJob %v to finish", job.Name))
				case <-tick.C:
				}

				if len(c.Status.Active) == 0 {
					break
				}

				c, err = client.BatchV1beta1().CronJobs(s.namespace).Get(ctx, c.Name, metav1.GetOptions{})
				if err != nil {
					return errors.Wrapf(err, "error getting update of CronJob %v's Status", job.Name)
				}
			}

			log.Debugf("%v/%v: suspended CronJob %v", s.namespace, s.pvc, job.Name)
			break
		}
	}
	return nil
}

func (s scaleUp) cronJobs(ctx context.Context, client kubernetes.Interface) error {
	jobs, err := client.BatchV1beta1().CronJobs(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	for _, job := range jobs.Items {
		if name, ok := job.Annotations[pvcMigration]; !ok || name != s.pvcName {
			continue
		}

		job.Spec.Suspend = nil

		delete(job.Annotations, pvcMigration)

		_, err := client.BatchV1beta1().CronJobs(s.namespace).Update(ctx, &job, metav1.UpdateOptions{})
		if err != nil {
			return errors.Wrapf(err, "could not suspend cronJob %v", job.Name)
		}
		log.Debugf("%v/%v: reenabled CronJob %v", s.namespace, s.pvc, job.Name)
	}
	return nil
}

// jobs waits until all Jobs have no active pods anymore. We can't really stop
// them, as we don't know what they are executing, so we wait...
func (s scaleDown) jobs(ctx context.Context, client kubernetes.Interface) error {
	jobs, err := client.BatchV1().Jobs(s.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}

	for _, job := range jobs.Items {
		// only process jobs that have no controlling owner
		if hasControllingOwner(job.OwnerReferences) {
			continue
		}

		for _, vol := range job.Spec.Template.Spec.Volumes {
			if vol.PersistentVolumeClaim == nil {
				continue
			}
			if vol.PersistentVolumeClaim.ClaimName != s.pvc {
				continue
			}

			// wait until jobs are finished
			j := &job
			tick := time.NewTicker(time.Second * 5)
			timeout := time.NewTimer(time.Minute * 10)
			for {
				select {
				case <-tick.C:
				case <-timeout.C:
					return errors.New(fmt.Sprintf("timeout waiting for job %v to complete", job.Name))
				}

				if j.Status.Active == 0 {
					break
				}

				time.Sleep(time.Second * 5)
				j, err = client.BatchV1().Jobs(s.namespace).Get(ctx, j.Name, metav1.GetOptions{})
				if err != nil {
					return errors.Wrapf(err, "error getting update of Job %v's Status", job.Name)
				}
				log.Debugf("%v/%v: waiting for Job %v to finish", s.namespace, s.pvc, job.Name)
			}
			log.Debugf("%v/%v: no instances of Job %v running anymore", s.namespace, s.pvc, job.Name)
			break
		}
	}
	return nil
}
