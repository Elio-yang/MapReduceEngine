package mr

import (
	"time"
)

const maxTaskLiveTime = time.Second * 10
const scheduleTime = time.Millisecond * 500

// stages for a Task
type taskStage int

const (
	Map = 0
	// Reduce when all map is Done
	Reduce = 1
)

// status for a Task
type taskStatus int

const (
	tStatusReady      = 0
	tStatusInProgress = 1
	tStatusDone       = 2
	tStatusIdle       = 3
	tStatusErr        = 4
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
