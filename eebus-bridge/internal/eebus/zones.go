package eebus

import (
	"strings"

	spineapi "github.com/enbility/spine-go/api"
)

// ZoneID identifies a heating zone by its remote entity's SPINE address,
// joined with dots, e.g. "5.1.1" for the HVACRoom entity 5:1:1. The address is
// stable across reconnects, unlike the entity objects themselves.
func ZoneID(entity spineapi.EntityRemoteInterface) string {
	if entity == nil {
		return ""
	}
	return strings.ReplaceAll(EntityAddressString(entity.Address()), ":", ".")
}

// CompareEntities orders remote entities numerically by address, so "2" sorts
// before "10" and "5:1:1" before "6:1:1".
func CompareEntities(a, b spineapi.EntityRemoteInterface) int {
	return compareEntityAddresses(a.Address(), b.Address())
}

// zoneContains reports whether the entity with entityID lies below the zone
// entity with zoneID, e.g. HeatingZone "5.1" contains HVACRoom "5.1.1".
func zoneContains(zoneID, entityID string) bool {
	return zoneID != "" && strings.HasPrefix(entityID, zoneID+".")
}
