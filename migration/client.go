package migration

import (
	"context"

	"github.com/pkg/errors"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type kubernetesClient struct {
	c kubernetes.Interface
}

var (
	getOpts    = metav1.GetOptions{}
	updateOpts = metav1.UpdateOptions{}
	createOpts = metav1.CreateOptions{}
	deleteOpts = metav1.DeleteOptions{}
	listOpts   = metav1.ListOptions{}
)

// CreateJob creates a Job and returns it after the server has processed it. It does not fail if the
// Job already exists, but grabs the already existing one.
func (k *kubernetesClient) CreateJob(ctx context.Context, job *batchv1.Job) (*batchv1.Job, error) {
	_, err := k.c.BatchV1().Jobs(job.Namespace).Create(ctx, job, createOpts)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, errors.Wrapf(err, "could not create Job")
		}
	}

	return k.c.BatchV1().Jobs(job.Namespace).Get(ctx, job.Name, metav1.GetOptions{})
}

func (k *kubernetesClient) GetJob(ctx context.Context, name, namespace string) (*batchv1.Job, error) {
	return k.c.BatchV1().Jobs(namespace).Get(ctx, name, getOpts)
}

func (k *kubernetesClient) GetPV(ctx context.Context, name string) (*corev1.PersistentVolume, error) {
	return k.c.CoreV1().PersistentVolumes().Get(ctx, name, getOpts)
}

func (k *kubernetesClient) UpdatePV(ctx context.Context, pv *corev1.PersistentVolume,
	updateFunc func(*corev1.PersistentVolume),
) (*corev1.PersistentVolume, error) {
	pv, err := k.c.CoreV1().PersistentVolumes().Get(ctx, pv.Name, getOpts)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get PVC to update")
	}

	updateFunc(pv)

	for {
		pv, err := k.c.CoreV1().PersistentVolumes().Update(ctx, pv, updateOpts)
		if err == nil || !apierrors.IsConflict(err) {
			return pv, err
		}

		updateFunc(pv)
	}
}

// CreatePVC creates a PVC and returns it after the server has processed it. It does not fail if the
// PVC already exists, but grabs the already existing one.
func (k *kubernetesClient) CreatePVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	_, err := k.c.CoreV1().PersistentVolumeClaims(pvc.Namespace).Create(ctx, pvc, createOpts)
	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return nil, errors.Wrapf(err, "could not create PVC")
		}
	}

	return k.c.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, metav1.GetOptions{})
}

func (k *kubernetesClient) GetPVC(ctx context.Context, name, namespace string) (*corev1.PersistentVolumeClaim, error) {
	return k.c.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, getOpts)
}

func (k *kubernetesClient) ListPVCs(ctx context.Context, namespace string) ([]corev1.PersistentVolumeClaim, error) {
	p, err := k.c.CoreV1().PersistentVolumeClaims(namespace).List(ctx, listOpts)
	return p.Items, err
}

func (k *kubernetesClient) DeletePVC(ctx context.Context, name, namespace string) error {
	return k.c.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, deleteOpts)
}

func (k *kubernetesClient) UpdatePVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim,
	updateFunc func(*corev1.PersistentVolumeClaim),
) (*corev1.PersistentVolumeClaim, error) {
	pvc, err := k.c.CoreV1().PersistentVolumeClaims(pvc.Namespace).Get(ctx, pvc.Name, getOpts)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get PVC to update")
	}

	updateFunc(pvc)

	for {
		pvc, err := k.c.CoreV1().PersistentVolumeClaims(pvc.Namespace).Update(ctx, pvc, updateOpts)
		if err == nil || !apierrors.IsConflict(err) {
			return pvc, err
		}

		updateFunc(pvc)
	}
}

func (k *kubernetesClient) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	return k.c.CoreV1().Namespaces().Get(ctx, name, getOpts)
}

func (k *kubernetesClient) ListNamespaces(ctx context.Context) ([]corev1.Namespace, error) {
	n, err := k.c.CoreV1().Namespaces().List(ctx, listOpts)
	return n.Items, err
}

func (k *kubernetesClient) UpdateNamespace(ctx context.Context, ns *corev1.Namespace,
	updateFunc func(*corev1.Namespace),
) (*corev1.Namespace, error) {
	ns, err := k.c.CoreV1().Namespaces().Get(ctx, ns.Name, getOpts)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get PVC to update")
	}

	updateFunc(ns)

	for {
		ns, err := k.c.CoreV1().Namespaces().Update(ctx, ns, updateOpts)
		if err == nil || !apierrors.IsConflict(err) {
			return ns, err
		}
		updateFunc(ns)
	}
}
