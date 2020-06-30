package migration

import (
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
)

type Option func(*Migrator) error

// AutoConfirmPrompts answers every prompt with yes
func AutoConfirmPrompts() Option {
	return func(m *Migrator) error {
		m.autoConfirmPrompts = true
		return nil
	}
}

// Namespace migrates only a specific namespace.
func Namespace(ns string) Option {
	return func(m *Migrator) error {
		n, err := m.client.GetNamespace(m.ctx, ns)
		if err != nil {
			return errors.Wrapf(err, "could not get namespace %v", ns)
		}

		m.state[n] = nil
		return nil
	}
}

// PerPersistentVolumeClaim migrates only a specific PVC in a specific namespace.
func PersistentVolumeClaim(ns, pvc string) Option {
	return func(m *Migrator) error {
		n, err := m.client.GetNamespace(m.ctx, ns)
		if err != nil {
			return errors.Wrapf(err, "could not get namespace %v", ns)
		}

		p, err := m.client.GetPVC(m.ctx, pvc, ns)
		if err != nil {
			return errors.Wrapf(err, "could not get PVC %v", pvc)
		}

		if m.state[n] == nil {
			m.state[n] = make(map[*corev1.PersistentVolumeClaim]*state)
		}

		m.state[n][p] = newState(n, p)
		return nil
	}
}

// TargetClass switches the target PVC storage class to the provided one.
// The default that is set is "filestore"
func TargetClass(class string) Option {
	return func(_ *Migrator) error {
		globalTargetClass = class
		return nil
	}
}

// SourceClass switches the source PVC storage class to the provided one.
// The default that is set is "nfs"
func SourceClass(class string) Option {
	return func(m *Migrator) error {
		globalSourceClass = class
		return nil
	}
}
