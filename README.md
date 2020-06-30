# Relokator

Relokator makes it easy to move data from StorageClass to another, for example
allowing you to migrate a zonal PV to a regional PV.

## Installation

If you have Go installed and enabled Go modules (`GO111MODULE="on"`):

```
$> go get github.com/ninech/relokator
$> relokator -h
```

Or with Docker (need to mount your kubeconfig credentials into the container):

```
docker run -v ~/.kube:/.kube ninech/relokator -h
```

## Usage

Before running this tool, make sure you have backed up your data!

There is downtime involved with the migration! We recommend running `relokator`
on a per-PVC basis and migrate your PVCs one by one. `relokator` will print
the PVCs that will be migrated before starting the migration and will ask for
confirmation.

`relokator` will wait until no pod has the PVC mounted anymore. It:
- Scales down Deployments, ReplicaSets and StatefulSets to 0 (and scale it up
  again when it's done)
- Suspends CronJobs so that they will not interfere
- Waits for Jobs (or already running CronJobs) to complete

It cannot handle DaemonSets. If you have a DaemonSet that mounts a PVC you want
to migrate, you will need to scale it down manually somehow.

The new PVC (the target of the migration) will have some annotations set for
[Argo CD](https://github.com/argoproj/argo-cd) in order to ensure that it
will not be reconciled and changed again. After the migration is complete,
get the new manifest for your PVC (`kubectl get pvc <name> -o yaml`), remove
the annotations

```
argocd.argoproj.io/sync-options: "Prune=false"
argocd.argoproj.io/compare-options: "IgnoreExtraneous"
```

and add the new manifest to your git. This will cause ArgoCD to reconcile
the PVC again.

Relokator is written in a way that allows it to be stopped and restarted flexibly.
This means if Relokator encounters an error, it is possible to retry by running
the same command again.

## Example Usage

To migrate the PVC `app-persistent` in namespace `my-super-app` from the source
StorageClass `nfs` to destination StorageClass `filestore`:

```
./relokator -namespace my-super-app -pvc app-persistent \
            -target-class "filestore" -source-class "nfs"
```

## Reporting Issues

If you are opening an issue, please ensure that you have the latest version
of `relokator` installed and supply the output with `-loglevel debug` enabled.
