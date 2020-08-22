package ddc

import (
	"github.com/Alluxio/pillars/pkg/ddc/alluxio"
	"github.com/Alluxio/pillars/pkg/ddc/base"
	"github.com/Alluxio/pillars/pkg/ddc/configs"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"fmt"
)

type buildFunc func(id string,
	config *configs.Config,
	client client.Client,
	log logr.Logger) (engine base.Engine, err error)

var buildFuncMap map[string]buildFunc

func init() {
	buildFuncMap = map[string]buildFunc{
		"alluxio": alluxio.Build,
	}
}

/**
* Build Engine from config
 */
func CreateEngine(id string,
	config *configs.Config,
	client client.Client,
	log logr.Logger) (engine base.Engine, err error) {

	if buildeFunc, found := buildFuncMap[config.Runtime]; found {
		engine, err = buildeFunc(id, config, client, log)
	} else {
		err = fmt.Errorf("Failed to build the engine due to the type %s is not found", config.Runtime)
	}

	return
}
