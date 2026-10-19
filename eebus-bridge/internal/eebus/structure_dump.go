package eebus

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	spineapi "github.com/enbility/spine-go/api"
	"github.com/enbility/spine-go/model"
)

// ErrRemoteDeviceNotConnected reports that no SPINE device is connected for a SKI.
var ErrRemoteDeviceNotConnected = errors.New("remote device not connected")

// Read status of one function in a DeviceStructure. The values are part of the
// dump's JSON format.
const (
	// ReadStatusRead: the device answered the read sent for this dump.
	ReadStatusRead = "read"
	// ReadStatusNoResponse: a read was sent but not confirmed before the deadline.
	ReadStatusNoResponse = "no_response"
	// ReadStatusFailed: the read could not be sent or the device returned an error.
	ReadStatusFailed = "failed"
	// ReadStatusNoLocalClient: the bridge has no client feature of this type to
	// read with. Creating one would change what the bridge announces to the
	// device, so the dump only reports the data already cached.
	ReadStatusNoLocalClient = "no_local_client"
	// ReadStatusNotReadable: the device does not permit reading this function.
	ReadStatusNotReadable = "not_readable"
)

// DeviceStructure is a read-only snapshot of everything a remote device exposes
// over SPINE: entity tree, features, functions with their permitted operations
// and current data, and advertised use cases.
type DeviceStructure struct {
	SKI        string             `json:"ski"`
	DeviceType string             `json:"device_type,omitempty"`
	Address    string             `json:"device_address,omitempty"`
	CapturedAt time.Time          `json:"captured_at"`
	Entities   []EntityStructure  `json:"entities"`
	UseCases   []UseCaseStructure `json:"use_cases"`
}

type EntityStructure struct {
	Address  string             `json:"address"`
	Type     string             `json:"type"`
	Features []FeatureStructure `json:"features"`
}

type FeatureStructure struct {
	Address     string              `json:"address"`
	Type        string              `json:"type"`
	Role        string              `json:"role"`
	Description string              `json:"description,omitempty"`
	Functions   []FunctionStructure `json:"functions"`
}

type FunctionStructure struct {
	Name       string `json:"name"`
	Operations string `json:"operations"`
	ReadStatus string `json:"read_status"`
	ReadError  string `json:"read_error,omitempty"`
	// Data is the SPINE model value cached for this function, nil if none.
	Data any `json:"data,omitempty"`
}

type UseCaseStructure struct {
	Actor     string `json:"actor"`
	Name      string `json:"name"`
	Version   string `json:"version"`
	Available bool   `json:"available"`
	Scenarios []uint `json:"scenarios"`
}

// StructureDumper builds DeviceStructure snapshots. It only sends SPINE read
// requests, never writes, and never adds features to the local device.
type StructureDumper struct {
	localDevice func() spineapi.DeviceLocalInterface
	readWait    time.Duration
	now         func() time.Time
}

// NewStructureDumper returns a dumper that waits up to readWait for the answers
// to its read requests. localDevice is resolved per dump because the local
// device only exists once the EEBUS service is set up.
func NewStructureDumper(localDevice func() spineapi.DeviceLocalInterface, readWait time.Duration) *StructureDumper {
	return &StructureDumper{localDevice: localDevice, readWait: readWait, now: time.Now}
}

// Dump reads every readable server function of the connected device for the
// SKI and returns the resulting structure.
func (d *StructureDumper) Dump(ctx context.Context, ski string) (*DeviceStructure, error) {
	var local spineapi.DeviceLocalInterface
	if d.localDevice != nil {
		local = d.localDevice()
	}
	if local == nil {
		return nil, ErrRemoteDeviceNotConnected
	}
	remote := connectedRemoteDevice(local, ski)
	if remote == nil {
		return nil, ErrRemoteDeviceNotConnected
	}

	outcomes := d.readAll(ctx, local, remote)
	return buildDeviceStructure(ski, remote, outcomes, d.now()), nil
}

// connectedRemoteDevice finds the remote device for a SKI. spine-go keys its
// remote devices by the SKI exactly as SHIP reported it, so compare normalized.
func connectedRemoteDevice(local spineapi.DeviceLocalInterface, ski string) spineapi.DeviceRemoteInterface {
	want := NormalizeSKI(ski)
	for _, remote := range local.RemoteDevices() {
		if remote != nil && NormalizeSKI(remote.Ski()) == want {
			return remote
		}
	}
	return nil
}

type functionKey struct {
	feature  string
	function model.FunctionType
}

type readOutcome struct {
	status string
	err    string
}

// readTracker collects the answers to the reads of one dump. done closes once
// all reads are issued and answered. Answers that arrive after the dump
// finished are ignored.
type readTracker struct {
	mu        sync.Mutex
	outcomes  map[functionKey]readOutcome
	pending   int
	allIssued bool
	finished  bool
	done      chan struct{}
}

func (t *readTracker) resolve(key functionKey, outcome readOutcome) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finished || t.outcomes[key].status != ReadStatusNoResponse {
		return
	}
	t.outcomes[key] = outcome
	t.pending--
	t.closeIfComplete()
}

// markAllIssued must be called once after the last read was requested.
func (t *readTracker) markAllIssued() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.allIssued = true
	t.closeIfComplete()
}

// closeIfComplete requires t.mu to be held.
func (t *readTracker) closeIfComplete() {
	if t.allIssued && t.pending == 0 {
		select {
		case <-t.done:
		default:
			close(t.done)
		}
	}
}

func (t *readTracker) finish() map[functionKey]readOutcome {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finished = true
	return t.outcomes
}

func (d *StructureDumper) readAll(
	ctx context.Context,
	local spineapi.DeviceLocalInterface,
	remote spineapi.DeviceRemoteInterface,
) map[functionKey]readOutcome {
	tracker := &readTracker{outcomes: make(map[functionKey]readOutcome), done: make(chan struct{})}

	for _, entity := range remote.Entities() {
		if entity == nil {
			continue
		}
		for _, feature := range entity.Features() {
			if feature == nil || feature.Role() != model.RoleTypeServer {
				continue
			}
			client := localClientFeature(local, feature.Type())
			for function, operations := range feature.Operations() {
				key := functionKey{feature: formatFeatureAddress(feature.Address()), function: function}
				switch {
				case operations == nil || !operations.Read():
					tracker.outcomes[key] = readOutcome{status: ReadStatusNotReadable}
				case client == nil:
					tracker.outcomes[key] = readOutcome{status: ReadStatusNoLocalClient}
				default:
					d.requestRead(tracker, client, feature, key)
				}
			}
		}
	}

	tracker.markAllIssued()
	timer := time.NewTimer(d.readWait)
	defer timer.Stop()
	select {
	case <-tracker.done:
	case <-timer.C:
	case <-ctx.Done():
	}
	return tracker.finish()
}

func (d *StructureDumper) requestRead(
	tracker *readTracker,
	client spineapi.FeatureLocalInterface,
	feature spineapi.FeatureRemoteInterface,
	key functionKey,
) {
	tracker.mu.Lock()
	tracker.outcomes[key] = readOutcome{status: ReadStatusNoResponse}
	tracker.pending++
	tracker.mu.Unlock()

	msgCounter, spineErr := client.RequestRemoteData(key.function, nil, nil, feature)
	if spineErr != nil || msgCounter == nil {
		tracker.resolve(key, readOutcome{status: ReadStatusFailed, err: spineErrorText(spineErr)})
		return
	}
	err := client.AddResponseCallback(*msgCounter, func(msg spineapi.ResponseMessage) {
		if result, ok := msg.Data.(*model.ResultDataType); ok && result != nil &&
			result.ErrorNumber != nil && *result.ErrorNumber != model.ErrorNumberTypeNoError {
			tracker.resolve(key, readOutcome{status: ReadStatusFailed, err: resultErrorText(result)})
			return
		}
		tracker.resolve(key, readOutcome{status: ReadStatusRead})
	})
	if err != nil {
		tracker.resolve(key, readOutcome{status: ReadStatusFailed, err: err.Error()})
	}
}

// localClientFeature returns an existing local client feature of the given
// type, or nil. It never creates one.
func localClientFeature(local spineapi.DeviceLocalInterface, featureType model.FeatureTypeType) spineapi.FeatureLocalInterface {
	for _, entity := range local.Entities() {
		if entity == nil {
			continue
		}
		if feature := entity.FeatureOfTypeAndRole(featureType, model.RoleTypeClient); feature != nil {
			return feature
		}
	}
	return nil
}

func spineErrorText(err *model.ErrorType) string {
	if err == nil {
		return "no message counter"
	}
	return err.String()
}

func resultErrorText(result *model.ResultDataType) string {
	text := fmt.Sprintf("error %d", uint(*result.ErrorNumber))
	if result.Description != nil {
		text += ": " + string(*result.Description)
	}
	return text
}

func buildDeviceStructure(
	ski string,
	remote spineapi.DeviceRemoteInterface,
	outcomes map[functionKey]readOutcome,
	capturedAt time.Time,
) *DeviceStructure {
	result := &DeviceStructure{
		SKI:        ShortSKI(ski),
		CapturedAt: capturedAt.UTC(),
		Entities:   []EntityStructure{},
		UseCases:   []UseCaseStructure{},
	}
	if deviceType := remote.DeviceType(); deviceType != nil {
		result.DeviceType = string(*deviceType)
	}
	if address := remote.Address(); address != nil {
		result.Address = string(*address)
	}

	entities := slices.DeleteFunc(slices.Clone(remote.Entities()), func(e spineapi.EntityRemoteInterface) bool { return e == nil })
	slices.SortFunc(entities, func(a, b spineapi.EntityRemoteInterface) int {
		return compareEntityAddresses(a.Address(), b.Address())
	})
	for _, entity := range entities {
		result.Entities = append(result.Entities, entityStructure(entity, outcomes))
	}

	for _, useCase := range remote.UseCases() {
		result.UseCases = append(result.UseCases, useCaseStructures(useCase)...)
	}
	slices.SortFunc(result.UseCases, func(a, b UseCaseStructure) int {
		return cmp.Or(cmp.Compare(a.Actor, b.Actor), cmp.Compare(a.Name, b.Name), cmp.Compare(a.Version, b.Version))
	})
	return result
}

func entityStructure(entity spineapi.EntityRemoteInterface, outcomes map[functionKey]readOutcome) EntityStructure {
	result := EntityStructure{
		Address:  formatEntityAddress(entity.Address()),
		Type:     string(entity.EntityType()),
		Features: []FeatureStructure{},
	}
	features := slices.DeleteFunc(slices.Clone(entity.Features()), func(f spineapi.FeatureRemoteInterface) bool { return f == nil })
	slices.SortFunc(features, func(a, b spineapi.FeatureRemoteInterface) int {
		return compareFeatureAddresses(a.Address(), b.Address())
	})
	for _, feature := range features {
		result.Features = append(result.Features, featureStructure(feature, outcomes))
	}
	return result
}

func featureStructure(feature spineapi.FeatureRemoteInterface, outcomes map[functionKey]readOutcome) FeatureStructure {
	address := formatFeatureAddress(feature.Address())
	result := FeatureStructure{
		Address:   address,
		Type:      string(feature.Type()),
		Role:      string(feature.Role()),
		Functions: []FunctionStructure{},
	}
	if description := feature.Description(); description != nil {
		result.Description = string(*description)
	}
	for function, operations := range feature.Operations() {
		outcome, ok := outcomes[functionKey{feature: address, function: function}]
		if !ok {
			// Client-role features are not read; their functions only describe
			// what the device itself requests.
			outcome = readOutcome{status: ReadStatusNotReadable}
		}
		result.Functions = append(result.Functions, FunctionStructure{
			Name:       string(function),
			Operations: formatOperations(operations),
			ReadStatus: outcome.status,
			ReadError:  outcome.err,
			Data:       feature.DataCopy(function),
		})
	}
	slices.SortFunc(result.Functions, func(a, b FunctionStructure) int { return cmp.Compare(a.Name, b.Name) })
	return result
}

func useCaseStructures(useCase model.UseCaseInformationDataType) []UseCaseStructure {
	actor := ""
	if useCase.Actor != nil {
		actor = string(*useCase.Actor)
	}
	result := make([]UseCaseStructure, 0, len(useCase.UseCaseSupport))
	for _, support := range useCase.UseCaseSupport {
		entry := UseCaseStructure{Actor: actor, Scenarios: []uint{}}
		if support.UseCaseName != nil {
			entry.Name = string(*support.UseCaseName)
		}
		if support.UseCaseVersion != nil {
			entry.Version = string(*support.UseCaseVersion)
		}
		if support.UseCaseAvailable != nil {
			entry.Available = *support.UseCaseAvailable
		}
		for _, scenario := range support.ScenarioSupport {
			entry.Scenarios = append(entry.Scenarios, uint(scenario))
		}
		slices.Sort(entry.Scenarios)
		result = append(result, entry)
	}
	return result
}

// compareEntityAddresses orders entity addresses numerically, so "2" sorts
// before "10" and "2:1" after "2".
func compareEntityAddresses(a, b *model.EntityAddressType) int {
	return slices.Compare(entityParts(a), entityParts(b))
}

func compareFeatureAddresses(a, b *model.FeatureAddressType) int {
	return slices.Compare(featureParts(a), featureParts(b))
}

func entityParts(addr *model.EntityAddressType) []uint {
	if addr == nil {
		return nil
	}
	parts := make([]uint, 0, len(addr.Entity))
	for _, part := range addr.Entity {
		parts = append(parts, uint(part))
	}
	return parts
}

func featureParts(addr *model.FeatureAddressType) []uint {
	if addr == nil {
		return nil
	}
	parts := make([]uint, 0, len(addr.Entity)+1)
	for _, part := range addr.Entity {
		parts = append(parts, uint(part))
	}
	if addr.Feature != nil {
		parts = append(parts, uint(*addr.Feature))
	}
	return parts
}
