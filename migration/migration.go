package migration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/op/go-logging"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	isRenamedPVC = "nine.ch/pvc-migration-renamed"
	// oldReplicaCount specifies how many replicas the resource
	// had before we scaled it down. If this is -1, it was unspecified
	oldReplicaCount = "nine.ch/pvc-migration-old-replicacount"
	// annotation to mark a compute resource to be tied to a specific
	// PVC's migration. used to scale the compute resource up & down
	pvcMigration = "nine.ch/pvc-migration"
	// tracks which phase we have last completed on this PVC
	completedMigrationPhase = "nine.ch/pvc-migration-phase"

	// suffix that is added to the renamed PVC
	migrationSuffix = "-migration"

	// The Key that is put in place of "true" on the ArgoCD admin
	// annotation
	migrationInProgress = "migration-in-progress"
)

var (
	globalSourceClass = "nfs"
	globalTargetClass = "filestore"

	log = logging.MustGetLogger("relokator")
)

type Migrator struct {
	ctx context.Context

	client kubernetesClient

	phases []phase

	state clusterState

	autoConfirmPrompts bool
}

type clusterState map[*corev1.Namespace]namespaceState

type namespaceState map[*corev1.PersistentVolumeClaim]*state

// state is the state of a single PVC migration
type state struct {
	ns         *corev1.Namespace
	sourcePV   *corev1.PersistentVolume
	sourcePVC  *corev1.PersistentVolumeClaim
	renamedPVC *corev1.PersistentVolumeClaim
	targetPVC  *corev1.PersistentVolumeClaim

	completedPhase string
}

func newState(ns *corev1.Namespace, pvc *corev1.PersistentVolumeClaim) *state {
	s := &state{
		ns: ns,
	}

	if _, ok := pvc.Annotations[isRenamedPVC]; ok {
		s.renamedPVC = pvc
	} else {
		s.sourcePVC = pvc
	}

	s.completedPhase = pvc.Annotations[completedMigrationPhase]

	return s
}

type phase struct {
	exec func(*state, context.Context, kubernetesClient) error
	string
}

func New(ctx context.Context, client kubernetes.Interface, sourceClass, targetClass string, opts ...Option) (*Migrator, error) {
	globalSourceClass = sourceClass
	globalTargetClass = targetClass

	m := &Migrator{
		ctx:                ctx,
		client:             kubernetesClient{client},
		autoConfirmPrompts: false,
		state:              make(clusterState),
		phases: []phase{
			{(*state).retainPolicy, "retainPolicy"},
			{(*state).scaleDown, "scaleDown"},
			{(*state).recreatePVC, "recreatePVC"},
			{(*state).switchTargetPVC, "switchTargetPVC"},
			{(*state).createTargetPVC, "createTargetPVC"},
			{(*state).migrateData, "migrateData"},
			{(*state).scaleUp, "scaleUp"},
			{(*state).cleanUp, "cleanUp"},
		},
	}

	// apply options
	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, errors.Wrapf(err, "could not apply option")
		}
	}

	return m, nil
}

// allNamespaces adds all namespaces to the state that should be migrated
func (c clusterState) allNamespaces(ctx context.Context, client kubernetesClient) error {
	n, err := client.ListNamespaces(ctx)
	if err != nil {
		return err
	}
	for _, ns := range n {
		// capture loop variable to get a proper address
		// if we didn't do this, all namespaces have the same,
		// re-used address.
		ns := ns
		c[&ns] = nil
	}
	return nil
}

// allPVCs adds all PVCs to the state that should be migrated
func (c clusterState) allPVCs(ctx context.Context, client kubernetesClient, namespace *corev1.Namespace) error {
	p, err := client.ListPVCs(ctx, namespace.Name)
	if err != nil {
		return errors.Wrapf(err, "could not list PVs")
	}
	for _, pvc := range p {
		if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != globalSourceClass {
			log.Debugf("%v/%v: storageClass of PVC is %q, not %q. not migrating", namespace.Name, pvc.Name, *pvc.Spec.StorageClassName, globalSourceClass)
			continue
		}
		// TODO: figure out if a PVC is in Terminating state and ignore it

		if c[namespace] == nil {
			c[namespace] = make(namespaceState)
		}
		c[namespace][&pvc] = newState(namespace, &pvc)
	}

	return nil
}

func (m *Migrator) String() string {
	debug := false
	if logging.GetLevel("") == logging.DEBUG {
		debug = true
	}

	var str strings.Builder

	w := tabwriter.NewWriter(&str, 6, 8, 2, ' ', 0)
	// header
	if debug {
		fmt.Fprint(w, "namespace\tPVC\tis renamed\tPV\tlast Phase\tnext Phase\n")
		fmt.Fprint(w, "---------\t------\t----------\t------\t----------\t----------\n")
	} else {
		fmt.Fprint(w, "namespace\tPVC\tPV\n")
		fmt.Fprint(w, "---------\t------\t------\n")
	}

	for ns, nsState := range m.state {
		for pvc, pvcState := range nsState {
			var isRenamed bool
			if pvcState.renamedPVC != nil {
				isRenamed = true
			}
			currentPhase, nextPhase := "none", "none"
			if pvcState.completedPhase != "" {
				currentPhase = pvcState.completedPhase
			}

			p := m.getPhases(pvcState.completedPhase)
			if len(p) != 0 {
				nextPhase = p[0].string
			}

			if debug {
				fmt.Fprintf(w, "%s\t%s\t%v\t%s\t%s\t%s\n", ns.Name, pvc.Name,
					isRenamed, pvc.Spec.VolumeName, currentPhase,
					nextPhase,
				)
			} else {
				fmt.Fprintf(w, "%s\t%s\t%s\n", ns.Name, pvc.Name, pvc.Spec.VolumeName)
			}
		}
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}

	return str.String()
}

// toggleArgoAdmin enables or disables the admin access for argo in the specified namespace
func (m *Migrator) toggleArgoAdmin(ns *corev1.Namespace) error {
	const argoAnnotation = "nine.ch/argo-admin"

	val, ok := ns.Annotations[argoAnnotation]
	if !ok {
		return nil
	}

	n, err := m.client.UpdateNamespace(m.ctx, ns, func(ns *corev1.Namespace) {
		switch val {
		case migrationInProgress:
			ns.Annotations[argoAnnotation] = "true"
		case "true":
			ns.Annotations[argoAnnotation] = migrationInProgress
		default:
			// nothing to toggle
		}
	})
	if err != nil {
		return errors.Wrapf(err, "error removing Argo as Admin")
	}

	// update the namespace, but don't change the pointer.
	*ns = *n
	return nil
}

func (m *Migrator) Migrate() error {
	if len(m.state) == 0 {
		log.Debugf("-/-: searching for all namespaces")
		if err := m.state.allNamespaces(m.ctx, m.client); err != nil {
			return errors.Wrapf(err, "could not setup cluster state")
		}
	}

	for namespace, nsState := range m.state {
		// if it's not nil, it has been populated by an option and we don't want to touch it
		if nsState != nil {
			log.Debugf("%v/-: namespace has non-nil state, migrating only predefined targets", namespace.Name)
			continue
		}

		log.Debugf("%v/-: getting all PVCs in namespace to migrate", namespace.Name)
		if err := m.state.allPVCs(m.ctx, m.client, namespace); err != nil {
			return errors.Wrapf(err, "could not create namespace state")
		}
	}

	// if there's no work, exit early
	if m.noWork() {
		log.Infof("no PVC found to migrate")
		return nil
	}

	if err := m.promptUser(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	var e Errors
	for ns, nsState := range m.state {
		if err := m.namespace(ns, nsState); err != nil {
			e.Add(err)
		}
	}

	return e.OrNil()
}

// noWork returns true if there is no PVC to migrate
func (m *Migrator) noWork() bool {
	if len(m.state) == 0 {
		return true
	}

	for _, s := range m.state {
		if len(s) != 0 {
			return false
		}
	}

	return true
}

func (m *Migrator) promptUser() error {
	log.Infof("Will migrate the following PVCs:\n\n%v\n---------------------", m.String())

	if m.autoConfirmPrompts {
		return nil
	}

	fmt.Println("Confirm (y)es / (n)o:")

	var confirm func() bool
	confirm = func() bool {
		var response string

		_, err := fmt.Scanln(&response)
		if err != nil {
			fmt.Println("please type (y)es or (n)o and then press enter:")
			return confirm()
		}

		switch strings.ToLower(response) {
		case "y", "yes":
			return true
		case "n", "no":
			return false
		default:
			fmt.Println("please type (y)es or (n)o and then press enter:")
			return confirm()
		}
	}
	if !confirm() {
		return errors.New("aborted by user input")
	}
	return nil
}

func (m *Migrator) namespace(ns *corev1.Namespace, nsState namespaceState) error {
	log.Infof("starting migration in namespace %v", ns.Name)

	if err := m.toggleArgoAdmin(ns); err != nil {
		return errors.Wrapf(err, "could not toggle Argo admin")
	}

	defer func() {
		if err := m.toggleArgoAdmin(ns); err != nil {
			log.Errorf("error toggling Argo Admin: %v", err)
		}
	}()

	var e Errors
	var wg sync.WaitGroup
	for pvc, pvcState := range nsState {
		wg.Add(1)
		go func(pvc *corev1.PersistentVolumeClaim, pvcState *state) {
			defer wg.Done()
			if err := m.runPhases(pvcState); err != nil {
				e.Add(ErrMigration{
					ns:  ns,
					err: err,
					pvc: pvc,
				})
			}
			log.Infof("%v/%v: migration completed", ns.Name, pvc.Name)
		}(pvc, pvcState)
	}

	wg.Wait()

	log.Infof("migration in namespace %v completed", ns.Name)
	return e.OrNil()
}

type Errors struct {
	e []error
	sync.Mutex
}

func (e *Errors) Add(errs ...error) {
	e.Lock()
	defer e.Unlock()
	e.e = append(e.e, errs...)
}

func (e *Errors) OrNil() error {
	e.Lock()
	defer e.Unlock()
	if len(e.e) == 0 {
		return nil
	}
	return e
}

func (e *Errors) Error() string {
	e.Lock()
	defer e.Unlock()
	return fmt.Sprintf("Errors while migrating: %v", e.e)
}

type ErrMigration struct {
	ns  *corev1.Namespace
	err error
	pvc *corev1.PersistentVolumeClaim
}

func (e ErrMigration) Error() string {
	return fmt.Sprintf("Migration Error for PVC %s in namespace %v: %v", e.pvc.Name, e.ns.Name, e.err)
}

func (m *Migrator) getPhases(val string) []phase {
	for i, phase := range m.phases {
		if phase.string == val {
			return m.phases[i+1:]
		}
	}
	return m.phases
}

func (m *Migrator) runPhases(s *state) error {
	pvc := s.sourcePVC
	if pvc == nil {
		pvc = s.renamedPVC
	}

	pv, err := m.client.GetPV(m.ctx, pvc.Spec.VolumeName)
	if err != nil || pv == nil {
		return errors.Wrapf(err, "could not get corresponding PV %s", pvc.Spec.VolumeName)
	}

	s.sourcePV = pv

	for _, phase := range m.getPhases(s.completedPhase) {
		log.Debugf("%v/%v: starting phase %q", s.ns.Name, pvc.Name, phase.string)
		if err := phase.exec(s, m.ctx, m.client); err != nil {
			return errors.Wrapf(err, "error in phase %v", phase.string)
		}

		log.Debugf("%v/%v: executed phase %q", s.ns.Name, pvc.Name, phase.string)
		if err := m.updateTracking(m.ctx, s, phase.string); err != nil {
			return errors.Wrapf(err, "could not update tracking for phase %v", phase.string)
		}
		log.Debugf("%v/%v: tracking updated", s.ns.Name, pvc.Name)
	}
	return nil
}

func (m *Migrator) updateTracking(ctx context.Context, s *state, phase string) error {
	for _, pvc := range []*corev1.PersistentVolumeClaim{s.sourcePVC, s.renamedPVC, s.targetPVC} {
		if pvc == nil {
			continue
		}

		p, err := m.client.UpdatePVC(ctx, pvc, func(pvc *corev1.PersistentVolumeClaim) {
			pvc.Annotations[completedMigrationPhase] = phase
		})
		if err != nil {
			return errors.Wrapf(err, "error updating phase on original PVC")
		}
		*pvc = *p
	}

	return nil
}
