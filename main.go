package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"

	"github.com/ninech/relokator/migration"
	"github.com/op/go-logging"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	log    = logging.MustGetLogger("relokator")
	format = logging.MustStringFormatter(`%{color}%{level:.4s} %{shortfile} ▶%{color:reset} %{message}`)
)

func main() {
	kubeconfigDefaultPath := ""
	if home := homeDir(); home != "" {
		kubeconfigDefaultPath = filepath.Join(home, ".kube", "config")
	}

	var ( // flags
		allNamespaces   = flag.Bool("all-namespaces", false, "migrate all namespaces")
		inClusterConfig = flag.Bool("in-cluster-config", false, "Clientset will be generated through the inClusterConfig")
		kubeconfig      = flag.String("kubeconfig", kubeconfigDefaultPath, "(optional) absolute path to the kubeconfig file")
		logLevel        = flag.String("loglevel", "info", `specify the log level. valid options are: "debug", "info", "error"`)
		ns              = flag.String("namespace", "", "select a specific namespace to migrate. If -pvc is not set, it will migrate every PVC in that namespace.")
		pvc             = flag.String("pvc", "", "select a specific PVC to migrate. -namespace must be set too")
		sourceClass     = flag.String("source-class", "", "the storageClass of the PVCs to migrate")
		targetClass     = flag.String("target-class", "", "the storageClass of the PVCs to migrate to")
		yesToAll        = flag.Bool("y", false, "answer every prompt with yes")
	)
	flag.Parse()

	setupLogging(*logLevel)

	cs, err := getClientset(*inClusterConfig, *kubeconfig)
	if err != nil {
		log.Fatalf("could not create clientset: %v", err)
	}

	opts := []migration.Option{
		migration.SourceClass(*sourceClass),
		migration.TargetClass(*targetClass),
	}

	if *allNamespaces && (*ns != "" || *pvc != "") {
		log.Fatalf("cannot specify target together with 'all-namespaces'")
	}

	if *ns == "" && *pvc != "" {
		log.Fatalf("cannot set PVC but not namespace")
	}

	if !*allNamespaces {
		if *ns == "" {
			log.Fatalf("no namespace specified and '--all-namespaces' not set")
		}

		if *pvc != "" {
			opts = append(opts, migration.PersistentVolumeClaim(*ns, *pvc))
		} else {
			opts = append(opts, migration.Namespace(*ns))
		}
	}

	if *yesToAll {
		opts = append(opts, migration.AutoConfirmPrompts())
	}

	if *sourceClass == "" || *targetClass == "" {
		log.Fatalf("-source-class and -target-class must be set!")
	}

	m, err := migration.New(context.Background(), cs, *sourceClass, *targetClass, opts...)
	if err != nil {
		log.Fatalf("could not setup migration: %v", err)
	}

	if err := m.Migrate(); err != nil {
		log.Fatalf("error running migration: %v", err)
	}
}

func getClientset(inCluster bool, kubecfg string) (*kubernetes.Clientset, error) {
	var config *rest.Config
	var err error
	if inCluster {
		config, err = rest.InClusterConfig()
	} else {
		config, err = clientcmd.BuildConfigFromFlags("", kubecfg)
	}
	if err != nil {
		log.Fatalf("could not get clientset Config: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return clientset, nil
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func setupLogging(level string) {
	b := logging.NewLogBackend(os.Stdout, "", 0)
	bformatter := logging.NewBackendFormatter(b, format)
	logging.SetBackend(bformatter)

	logging.AddModuleLevel(b)

	loglevel, err := logging.LogLevel(level)
	if err != nil {
		log.Error("unrecognised log level, using info")
		loglevel = logging.INFO
	}
	logging.SetLevel(loglevel, "")
}
