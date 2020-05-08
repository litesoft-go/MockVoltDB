package canned

import (
	"github.com/litesoft-go/mockvoltdb/pkg/utils"
)

func PortResonses_http() (r *utils.Responder) {
	r = &utils.Responder{}
	r.AddPath("/XXX").
		Add(`{
	"clusterState": "RUNNING",
	"nodeState": "UP",
	"shutdownPending": false
}`).
		NoMore()
	return
}
