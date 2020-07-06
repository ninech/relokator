package migration

import (
	"bytes"
	"context"
	"math/rand"
	"strconv"
	"time"

	"github.com/pkg/errors"
	app "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

type testEnvironment struct {
	initial      []runtime.Object
	options      []Option
	preMigration assertion
	assertion    assertion
	// the number of expected phases
	expectedPhases int
	testState
}

type testState struct {
	pvc,
	pv,
	namespace,
	deployment string
	deploymentReplicas int32
}

// tests map a namespace name to a testEnvironment.
// This means that every test will run in its own environment
var tests = map[string]testEnvironment{}

func init() {
	for _, env := range []testEnvironment{
		standardTest(),
		fromMigration(),
	} {
		tests[env.namespace] = env
	}
}

var (
	testOldStorageClass  = "local-old"
	testNewStorageClass  = "local-new"
	waitForFirstConsumer = storage.VolumeBindingWaitForFirstConsumer

	// globalResources are to be applied only once and are valid
	// for all tests.
	globalResources = []runtime.Object{
		&storage.StorageClass{
			ObjectMeta: meta.ObjectMeta{
				Name: testOldStorageClass,
			},
			Provisioner:       "kubernetes.io/no-provisioner",
			VolumeBindingMode: &waitForFirstConsumer,
		},
		&storage.StorageClass{
			ObjectMeta: meta.ObjectMeta{
				Name: testNewStorageClass,
			},
			Provisioner:       "kubernetes.io/no-provisioner",
			VolumeBindingMode: &waitForFirstConsumer,
		},
	}
)

func assertStorageClass(ctx context.Context, pvcName, namespace, targetClass string, kc kubernetesClient) error {
	pvc, err := kc.GetPVC(ctx, pvcName, namespace)
	if err != nil {
		return err
	}
	if pvc.Spec.StorageClassName == nil {
		return errors.Errorf("Final Storage Class not correct. expected=%v, got=<nil>", testNewStorageClass)
	}
	if *pvc.Spec.StorageClassName != testNewStorageClass {
		return errors.Errorf("Final Storage Class not correct. expected=%v, got=%v",
			testNewStorageClass, *pvc.Spec.StorageClassName)
	}
	return nil
}

func (ts testState) bindVolume(ctx context.Context, kc kubernetesClient) error {
	pvc, err := kc.c.CoreV1().PersistentVolumeClaims(ts.namespace).Get(ctx, ts.pvc, meta.GetOptions{})
	if err != nil {
		return err
	}

	pvc.Status = core.PersistentVolumeClaimStatus{
		Phase: core.ClaimBound,
	}

	_, err = kc.c.CoreV1().PersistentVolumeClaims(ts.namespace).UpdateStatus(ctx, pvc, meta.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func fromMigration() testEnvironment {
	jobName := "random-name-" + uniqueID()
	env := testEnvironment{
		testState: testState{
			pvc:                "copydata-pvc",
			pv:                 "pvc-ae45612a-10ce-11ea-b278-22010aac003a",
			namespace:          "copydata",
			deployment:         "my-super-app",
			deploymentReplicas: 5,
		},
		assertion: func(ts testState, ctx context.Context, kc kubernetesClient) error {
			if err := assertStorageClass(ctx, ts.pvc, ts.namespace, testNewStorageClass, kc); err != nil {
				return err
			}
			return nil
		},
		expectedPhases: 3, // migrateData, scaleUp, cleanUp
		preMigration: func(ts testState, ctx context.Context, kc kubernetesClient) error {
			if err := ts.bindVolume(ctx, kc); err != nil {
				return err
			}

			return testState{
				namespace:  ts.namespace,
				deployment: ts.deployment,
				pvc:        withMigrationSuffix(ts.pvc),
				pv:         withMigrationSuffix(ts.pv),
			}.bindVolume(ctx, kc)
		},
	}
	env.options = []Option{Namespace(env.namespace)}
	env.initial = []runtime.Object{
		&core.Namespace{
			ObjectMeta: meta.ObjectMeta{
				Name: env.namespace,
				Annotations: map[string]string{
					argoAnnotation: migrationInProgress,
				},
			},
		},
		&core.PersistentVolume{
			ObjectMeta: meta.ObjectMeta{
				// technically, it will just be another ID.
				// But this makes it deterministic for the test.
				Name: withMigrationSuffix(env.pv),
			},
			Spec: core.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: core.PersistentVolumeReclaimDelete,
				StorageClassName:              testOldStorageClass,
				AccessModes:                   []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				ClaimRef: &core.ObjectReference{
					Namespace: env.namespace,
					Name:      env.pvc,
				},
				Capacity: core.ResourceList{
					core.ResourceStorage: resource.MustParse("10Gi"),
				},
				PersistentVolumeSource: core.PersistentVolumeSource{
					HostPath: &core.HostPathVolumeSource{
						Path: "/tmp/whatever", // not relevant
					},
				},
			},
		},
		&core.PersistentVolume{
			ObjectMeta: meta.ObjectMeta{
				Name: env.pv,
			},
			Spec: core.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: core.PersistentVolumeReclaimDelete,
				StorageClassName:              testNewStorageClass,
				AccessModes:                   []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				ClaimRef: &core.ObjectReference{
					Namespace: env.namespace,
					Name:      env.pvc,
				},
				Capacity: core.ResourceList{
					core.ResourceStorage: resource.MustParse("10Gi"),
				},
				PersistentVolumeSource: core.PersistentVolumeSource{
					HostPath: &core.HostPathVolumeSource{
						Path: "/tmp/whatever", // not relevant
					},
				},
			},
		},
		&core.PersistentVolumeClaim{
			ObjectMeta: meta.ObjectMeta{
				Name:      withMigrationSuffix(env.pvc),
				Namespace: env.namespace,
				Annotations: map[string]string{
					isRenamedPVC:            env.pvc,
					migrationJob:            jobName,
					completedMigrationPhase: "createTargetPVC",
				},
			},
			Spec: core.PersistentVolumeClaimSpec{
				StorageClassName: &testOldStorageClass,
				VolumeName:       withMigrationSuffix(env.pv),
				AccessModes:      []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				Resources: core.ResourceRequirements{
					Requests: core.ResourceList{
						core.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		},
		&core.PersistentVolumeClaim{
			ObjectMeta: meta.ObjectMeta{
				Name:      env.pvc,
				Namespace: env.namespace,
				Annotations: map[string]string{
					completedMigrationPhase: "createTargetPVC",
				},
			},
			Spec: core.PersistentVolumeClaimSpec{
				StorageClassName: &testNewStorageClass,
				VolumeName:       env.pv,
				AccessModes:      []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				Resources: core.ResourceRequirements{
					Requests: core.ResourceList{
						core.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		},
		newJobWithName(jobName, withMigrationSuffix(env.pvc), env.pvc, env.namespace),
		&core.Pod{
			ObjectMeta: meta.ObjectMeta{
				Name:      jobName + "-pod",
				Namespace: env.namespace,
				OwnerReferences: []meta.OwnerReference{
					{
						APIVersion: "batch/v1",
						Kind:       "Job",
						Name:       jobName,
						UID:        types.UID("whatever"),
					},
				},
			},
			Spec: core.PodSpec{
				Containers: []core.Container{
					{
						Image: "nginx",
						Name:  "nginx",
					},
				},
				Volumes: []core.Volume{
					{
						Name: "test-target",
						VolumeSource: core.VolumeSource{
							PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
								ClaimName: env.pvc,
								ReadOnly:  false,
							},
						},
					},
					{
						Name: "test-source",
						VolumeSource: core.VolumeSource{
							PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
								ClaimName: withMigrationSuffix(env.pvc),
								ReadOnly:  true,
							},
						},
					},
				},
			},
		},
		&app.Deployment{
			ObjectMeta: meta.ObjectMeta{
				Name:      env.deployment,
				Namespace: env.namespace,
				Annotations: map[string]string{
					pvcMigration:    env.pvc,
					oldReplicaCount: strconv.Itoa(int(env.deploymentReplicas)),
				},
			},
			Spec: app.DeploymentSpec{
				Replicas: newInt(0),
				Selector: &meta.LabelSelector{
					MatchLabels: map[string]string{
						"deploy": "label",
					},
				},
				Template: core.PodTemplateSpec{
					ObjectMeta: meta.ObjectMeta{
						Labels: map[string]string{"deploy": "label"},
					},
					Spec: core.PodSpec{
						Containers: []core.Container{
							{
								Image: "nginx",
								Name:  "nginx",
							},
						},
						Volumes: []core.Volume{
							{
								Name: "test",
								VolumeSource: core.VolumeSource{
									PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
										ClaimName: env.pvc,
										ReadOnly:  false,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	return env
}

func standardTest() testEnvironment {
	env := testEnvironment{
		assertion: func(ts testState, ctx context.Context, kc kubernetesClient) error {
			if err := assertStorageClass(ctx, ts.pvc, ts.namespace, testNewStorageClass, kc); err != nil {
				return err
			}
			return nil
		},
		expectedPhases: 8,
		testState: testState{
			pvc:        "test-pvc",
			pv:         "pvc-f9391fb1-1085-11ea-b278-22010aac003a",
			namespace:  "test-namespace",
			deployment: "test-deployment",
		},
		preMigration: testState.bindVolume,
	}
	env.options = []Option{PersistentVolumeClaim(env.namespace, env.pvc)}
	env.initial = []runtime.Object{
		&core.Namespace{
			ObjectMeta: meta.ObjectMeta{
				Name: env.namespace,
			},
		},
		&core.PersistentVolume{
			ObjectMeta: meta.ObjectMeta{
				Name: env.pv,
			},
			Spec: core.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: core.PersistentVolumeReclaimDelete,
				StorageClassName:              testOldStorageClass,
				AccessModes:                   []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				ClaimRef: &core.ObjectReference{
					Namespace: env.namespace,
					Name:      env.pvc,
				},
				Capacity: core.ResourceList{
					core.ResourceStorage: resource.MustParse("10Gi"),
				},
				PersistentVolumeSource: core.PersistentVolumeSource{
					HostPath: &core.HostPathVolumeSource{
						Path: "/tmp/whatever", // not relevant
					},
				},
			},
		},
		&core.PersistentVolumeClaim{
			ObjectMeta: meta.ObjectMeta{
				Name:      env.pvc,
				Namespace: env.namespace,
			},
			Spec: core.PersistentVolumeClaimSpec{
				StorageClassName: &testOldStorageClass,
				VolumeName:       env.pv,
				AccessModes:      []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				Resources: core.ResourceRequirements{
					Requests: core.ResourceList{
						core.ResourceStorage: resource.MustParse("10Gi"),
					},
				},
			},
		},
		&app.Deployment{
			ObjectMeta: meta.ObjectMeta{
				Name:      env.deployment,
				Namespace: env.namespace,
			},
			Spec: app.DeploymentSpec{
				Replicas: newInt(3),
				Selector: &meta.LabelSelector{
					MatchLabels: map[string]string{
						"deploy": "label",
					},
				},
				Template: core.PodTemplateSpec{
					ObjectMeta: meta.ObjectMeta{
						Labels: map[string]string{"deploy": "label"},
					},
					Spec: core.PodSpec{
						Containers: []core.Container{
							{
								Image: "nginx",
								Name:  "nginx",
							},
						},
						Volumes: []core.Volume{
							{
								Name: "test",
								VolumeSource: core.VolumeSource{
									PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
										ClaimName: env.pvc,
										ReadOnly:  false,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	return env
}

func newJobWithName(name, sourcePVC, targetPVC, namespace string) *batchv1.Job {
	j := newJob(sourcePVC, targetPVC, namespace)
	j.Name = name
	return j
}

// uniqueID returns a semi-unique ID.
func uniqueID() string {
	// shamelessly stolen from terratest.
	const base62chars = "0123456789abcdefghijklmnopqrstuvwxyz"
	const length = 10 // length of the ID, 6 should be good enough.
	var out bytes.Buffer

	generator := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < length; i++ {
		out.WriteByte(base62chars[generator.Intn(len(base62chars))])
	}

	return out.String()
}
