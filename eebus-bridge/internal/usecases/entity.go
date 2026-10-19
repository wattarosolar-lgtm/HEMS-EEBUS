package usecases

import (
	"slices"

	eebusapi "github.com/enbility/eebus-go/api"
	spineapi "github.com/enbility/spine-go/api"

	"github.com/volschin/eebus-bridge/internal/eebus"
)

func observationSKI(ski string, device spineapi.DeviceRemoteInterface) string {
	if eebus.NormalizeSKI(ski) == "" && device != nil {
		return device.Ski()
	}
	return ski
}

func recordCapabilitySupport(
	registry *eebus.DeviceRegistry,
	ski string,
	device spineapi.DeviceRemoteInterface,
	callbackEntity spineapi.EntityRemoteInterface,
	resolution eebus.EntityResolution,
	useCase string,
	capability eebus.Capability,
) bool {
	if registry == nil {
		return false
	}
	ski = observationSKI(ski, device)
	advertised := resolution.Entity != nil
	if connected, _, known := registry.DeviceConnection(ski); known && !connected {
		registry.RemoveEntityObservation(ski, callbackEntity)
		registry.RecordCapabilitySourceSupport(ski, capability, useCase, advertised)
		return false
	}
	if callbackEntity != nil && callbackEntity != resolution.Entity {
		registry.RemoveEntityObservation(ski, callbackEntity)
	}
	if advertised {
		registry.UpsertObservation(ski, device, resolution.Entity, useCase)
	} else {
		registry.RemoveEntityObservation(ski, callbackEntity)
	}
	registry.RecordCapabilitySourceSupport(ski, capability, useCase, advertised)
	return advertised
}

// compatibleEntity resolves the remote entity for a device SKI from a use
// case's negotiated scenario list instead of the flat device registry:
// multi-entity gateways expose several entities per device and only the
// scenario list is use-case aware (issue #47). An empty ski resolves only when
// exactly one distinct normalized device SKI is compatible. SKIs are compared
// normalized so formatting differences do not create false mismatches or
// ambiguity.
func compatibleEntity(scenarios []eebusapi.RemoteEntityScenarios, ski string) eebus.EntityResolution {
	want := eebus.NormalizeSKI(ski)
	if want != "" {
		for _, remote := range scenarios {
			entity := remote.Entity
			if entity == nil || entity.Device() == nil {
				continue
			}
			if eebus.NormalizeSKI(entity.Device().Ski()) == want {
				return eebus.EntityResolution{Entity: entity, DeviceCount: 1}
			}
		}
		return eebus.EntityResolution{}
	}

	entitiesBySKI := make(map[string]spineapi.EntityRemoteInterface)
	for _, remote := range scenarios {
		entity := remote.Entity
		if entity == nil || entity.Device() == nil {
			continue
		}
		normalizedSKI := eebus.NormalizeSKI(entity.Device().Ski())
		if _, seen := entitiesBySKI[normalizedSKI]; !seen {
			entitiesBySKI[normalizedSKI] = entity
		}
	}
	if len(entitiesBySKI) == 1 {
		for _, entity := range entitiesBySKI {
			return eebus.EntityResolution{Entity: entity, DeviceCount: 1}
		}
	}
	return eebus.EntityResolution{DeviceCount: len(entitiesBySKI)}
}

// compatibleEntities returns every entity of the device with this SKI from a
// use case's negotiated scenario list, in numeric address order. A device with
// several heating zones exposes one room entity per zone; the order stays
// stable across reconnects. An empty SKI lists nothing: callers resolve the
// device first, so an ambiguous lookup can never pick a zone.
func compatibleEntities(scenarios []eebusapi.RemoteEntityScenarios, ski string) []spineapi.EntityRemoteInterface {
	want := eebus.NormalizeSKI(ski)
	if want == "" {
		return nil
	}
	var entities []spineapi.EntityRemoteInterface
	for _, remote := range scenarios {
		entity := remote.Entity
		if entity == nil || entity.Device() == nil || eebus.NormalizeSKI(entity.Device().Ski()) != want {
			continue
		}
		if !slices.Contains(entities, entity) {
			entities = append(entities, entity)
		}
	}
	slices.SortStableFunc(entities, eebus.CompareEntities)
	return entities
}

// sameZoneEntity picks the candidate in the same zone as entity, i.e. at the
// same address. Use cases negotiate their entities independently, so after a
// reconnect the objects differ while the addresses stay the same. A single
// candidate is compared as well: while one use case has negotiated only some
// zones, it may be another zone's entity, and a write must never reach it.
func sameZoneEntity(
	candidates []spineapi.EntityRemoteInterface,
	entity spineapi.EntityRemoteInterface,
) spineapi.EntityRemoteInterface {
	if len(candidates) == 0 {
		return nil
	}
	zone := eebus.ZoneID(entity)
	if zone == "" {
		// Without an address the zone is unknown: fail closed.
		return nil
	}
	for _, candidate := range candidates {
		if eebus.ZoneID(candidate) == zone {
			return candidate
		}
	}
	return nil
}

func compatibleEntityForScenario(
	scenarios []eebusapi.RemoteEntityScenarios,
	ski string,
	scenario uint,
) eebus.EntityResolution {
	matching := make([]eebusapi.RemoteEntityScenarios, 0, len(scenarios))
	for _, remote := range scenarios {
		for _, available := range remote.Scenarios {
			if available == scenario {
				matching = append(matching, remote)
				break
			}
		}
	}
	return compatibleEntity(matching, ski)
}

func entityPresentInScenarios(
	scenarios []eebusapi.RemoteEntityScenarios,
	entity spineapi.EntityRemoteInterface,
) bool {
	if entity == nil {
		return false
	}
	for _, remote := range scenarios {
		if remote.Entity == entity {
			return true
		}
	}
	return false
}
