package migration

import (
	"github.com/pkg/errors"
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
		if m.state == nil {
			m.state = make(map[string]namespaceState)
		}

		m.state[ns] = nil
		return nil
	}
}

// PerPersistentVolumeClaim migrates only a specific PVC in a specific namespace.
func PersistentVolumeClaim(ns, pvc string) Option {
	return func(m *Migrator) error {
		_ = Namespace(ns)(m)
		if m.state[ns] == nil {
			m.state[ns] = make(map[string]*state)
		}

		st, err := newState(m.ctx, m.client, ns, pvc)
		if err != nil {
			return errors.Wrapf(err, "could not create state")
		}
		m.state[ns][pvc] = st
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
