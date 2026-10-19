package grpc

import (
	"context"
	"testing"

	eebusapi "github.com/enbility/eebus-go/api"
	spineapi "github.com/enbility/spine-go/api"
	"github.com/enbility/spine-go/mocks"
	"github.com/enbility/spine-go/model"
	pb "github.com/volschin/eebus-bridge/gen/proto/eebus/v1"
	"github.com/volschin/eebus-bridge/internal/eebus"
	"github.com/volschin/eebus-bridge/internal/usecases"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// zonedRoomHeatingTemp is a CRHT source that lists one entity per zone and
// validates writes against each zone's own limits, like the CRHT facade.
type zonedRoomHeatingTemp struct {
	entities []spineapi.EntityRemoteInterface
	// single, when set, answers the single-entity lookup regardless of entities.
	single      *eebus.EntityResolution
	states      map[spineapi.EntityRemoteInterface]usecases.RoomHeatingSetpoint
	writeEntity spineapi.EntityRemoteInterface
	writeValue  float64
}

func (f *zonedRoomHeatingTemp) CompatibleEntity(string) eebus.EntityResolution {
	if f.single != nil {
		return *f.single
	}
	if len(f.entities) == 0 {
		return eebus.EntityResolution{}
	}
	return eebus.EntityResolution{Entity: f.entities[0], DeviceCount: 1}
}

func (f *zonedRoomHeatingTemp) CompatibleEntities(string) []spineapi.EntityRemoteInterface {
	return f.entities
}

func (f *zonedRoomHeatingTemp) State(entity spineapi.EntityRemoteInterface) (usecases.RoomHeatingSetpoint, error) {
	state, ok := f.states[entity]
	if !ok {
		return usecases.RoomHeatingSetpoint{}, usecases.ErrRoomHeatingDataUnavailable
	}
	return state, nil
}

func (f *zonedRoomHeatingTemp) Write(_ context.Context, entity spineapi.EntityRemoteInterface, value float64) error {
	state := f.states[entity]
	if value < state.Minimum || value > state.Maximum {
		return usecases.ErrRoomHeatingOutOfRange
	}
	f.writeEntity = entity
	f.writeValue = value
	return nil
}

type zonedRoomSysFn struct {
	entities []spineapi.EntityRemoteInterface
	// single, when set, answers the single-entity lookup regardless of entities.
	single      *eebus.EntityResolution
	states      map[spineapi.EntityRemoteInterface]usecases.RoomHeatingSystemFunctionState
	writeEntity spineapi.EntityRemoteInterface
	writeMode   string
}

func (f *zonedRoomSysFn) CompatibleEntity(string) eebus.EntityResolution {
	if f.single != nil {
		return *f.single
	}
	if len(f.entities) == 0 {
		return eebus.EntityResolution{}
	}
	return eebus.EntityResolution{Entity: f.entities[0], DeviceCount: 1}
}

func (f *zonedRoomSysFn) CompatibleEntities(string) []spineapi.EntityRemoteInterface {
	return f.entities
}

func (f *zonedRoomSysFn) State(entity spineapi.EntityRemoteInterface) (usecases.RoomHeatingSystemFunctionState, error) {
	return f.states[entity], nil
}

func (f *zonedRoomSysFn) WriteOperationMode(_ context.Context, entity spineapi.EntityRemoteInterface, mode string) error {
	f.writeEntity = entity
	f.writeMode = mode
	return nil
}

// zonedRoomTemperature reports a room temperature only for zones with a valid
// sensor value, like MRT for a zone whose sensor reports an error state.
type zonedRoomTemperature struct {
	entities []spineapi.EntityRemoteInterface
	values   map[spineapi.EntityRemoteInterface]float64
}

func (f zonedRoomTemperature) Temperature(string) (float64, error) {
	return 0, eebusapi.ErrDataNotAvailable
}

func (f zonedRoomTemperature) CompatibleEntities(string) []spineapi.EntityRemoteInterface {
	return f.entities
}

func (f zonedRoomTemperature) EntityTemperature(entity spineapi.EntityRemoteInterface) (float64, error) {
	value, ok := f.values[entity]
	if !ok {
		return 0, eebusapi.ErrDataNotAvailable
	}
	return value, nil
}

func roomEntity(t *testing.T, ski string, address ...model.AddressEntityType) *mocks.EntityRemoteInterface {
	t.Helper()
	device := mocks.NewDeviceRemoteInterface(t)
	device.EXPECT().Ski().Return(ski).Maybe()
	entity := mocks.NewEntityRemoteInterface(t)
	entity.EXPECT().Address().Return(&model.EntityAddressType{Entity: address}).Maybe()
	entity.EXPECT().Device().Return(device).Maybe()
	return entity
}

type twoZones struct {
	svc            *HVACService
	temp           *zonedRoomHeatingTemp
	sysfn          *zonedRoomSysFn
	zone2, zone10  spineapi.EntityRemoteInterface
	registry       *eebus.DeviceRegistry
	livingRoomTemp float64
}

// newTwoZones builds a device with zones at addresses 10.1.1 and 2.1.1, listed
// in that order, so every test also checks the numeric address order. Zone
// 10.1.1 has no valid room temperature, like Zone 2 on the Vaillant gateway.
func newTwoZones(t *testing.T) twoZones {
	t.Helper()
	zone10 := roomEntity(t, testValidSKI, 10, 1, 1)
	zone2 := roomEntity(t, testValidSKI, 2, 1, 1)
	entities := []spineapi.EntityRemoteInterface{zone10, zone2}
	temp := &zonedRoomHeatingTemp{
		entities: entities,
		states: map[spineapi.EntityRemoteInterface]usecases.RoomHeatingSetpoint{
			zone2:  {Value: 21, Minimum: 5, Maximum: 30, Step: 0.5, Writable: true},
			zone10: {Value: 23, Minimum: 10, Maximum: 25, Step: 0.5, Writable: true},
		},
	}
	sysfn := &zonedRoomSysFn{
		entities: entities,
		states: map[spineapi.EntityRemoteInterface]usecases.RoomHeatingSystemFunctionState{
			zone2:  {OperationMode: "auto", AvailableModes: []string{"auto", "on", "off"}, ModeWritable: true},
			zone10: {OperationMode: "off", AvailableModes: []string{"auto", "on", "off"}, ModeWritable: true},
		},
	}
	room := zonedRoomTemperature{
		entities: entities,
		values:   map[spineapi.EntityRemoteInterface]float64{zone2: 23.5},
	}
	registry := eebus.NewDeviceRegistry()
	registry.UpsertZoneLabel(testValidSKI, "2.1", "Zone 1")
	registry.UpsertZoneLabel(testValidSKI, "10.1", "Zone 2")
	return twoZones{
		svc:            NewHVACService(temp, sysfn, room, nil, registry),
		temp:           temp,
		sysfn:          sysfn,
		zone2:          zone2,
		zone10:         zone10,
		registry:       registry,
		livingRoomTemp: 23.5,
	}
}

func TestHVACServiceListsEveryZoneInAddressOrder(t *testing.T) {
	zones := newTwoZones(t)

	list, err := zones.svc.ListRoomHeatingZones(context.Background(), &pb.DeviceRequest{Ski: testValidSKI})
	if err != nil {
		t.Fatalf("ListRoomHeatingZones: %v", err)
	}
	got := list.GetZones()
	if len(got) != 2 || got[0].GetZoneId() != "2.1.1" || got[1].GetZoneId() != "10.1.1" {
		t.Fatalf("zones = %v, want 2.1.1 then 10.1.1", got)
	}
	first, second := got[0].GetState(), got[1].GetState()
	if first.GetZoneLabel() != "Zone 1" || first.GetSetpoint().GetValueCelsius() != 21 ||
		first.GetSystemFunction().GetOperationMode() != "auto" || first.GetCurrentTemperatureCelsius() != zones.livingRoomTemp {
		t.Errorf("zone 2.1.1 = %v", first)
	}
	if second.GetZoneLabel() != "Zone 2" || second.GetSetpoint().GetValueCelsius() != 23 ||
		second.GetSetpoint().GetMinCelsius() != 10 || second.GetSystemFunction().GetOperationMode() != "off" {
		t.Errorf("zone 10.1.1 = %v", second)
	}
	if second.CurrentTemperatureCelsius != nil {
		t.Errorf("zone 10.1.1 room temperature = %v, want none", second.GetCurrentTemperatureCelsius())
	}

	// An empty SKI resolves to the only device.
	list, err = zones.svc.ListRoomHeatingZones(context.Background(), &pb.DeviceRequest{})
	if err != nil || len(list.GetZones()) != 2 {
		t.Fatalf("ListRoomHeatingZones(empty SKI) = %v, %v", list, err)
	}

	// The snapshot carries the same zones.
	if snapshot := zones.svc.RoomHeatingZones(testValidSKI); len(snapshot) != 2 || snapshot[1].GetZoneId() != "10.1.1" {
		t.Fatalf("RoomHeatingZones = %v", snapshot)
	}
}

// Single-zone clients get the first zone, with values and label of that zone
// only: never the label of one zone over the values of another.
func TestHVACServiceGetRoomHeatingServesTheFirstZone(t *testing.T) {
	zones := newTwoZones(t)

	state, err := zones.svc.GetRoomHeating(context.Background(), &pb.DeviceRequest{Ski: testValidSKI})
	if err != nil {
		t.Fatalf("GetRoomHeating: %v", err)
	}
	if state.GetZoneLabel() != "Zone 1" || state.GetSetpoint().GetValueCelsius() != 21 {
		t.Fatalf("GetRoomHeating = %v, want zone 2.1.1", state)
	}
}

func TestHVACServiceWritesExactlyTheNamedZone(t *testing.T) {
	zones := newTwoZones(t)
	ctx := context.Background()

	_, err := zones.svc.SetRoomHeatingTemperature(ctx, &pb.SetRoomHeatingTemperatureRequest{Ski: testValidSKI, ValueCelsius: 22})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("write without zone_id error = %v, want FailedPrecondition", err)
	}
	if zones.temp.writeEntity != nil {
		t.Fatal("write without zone_id reached a zone")
	}

	_, err = zones.svc.SetRoomHeatingTemperature(ctx, &pb.SetRoomHeatingTemperatureRequest{
		Ski: testValidSKI, ValueCelsius: 22, ZoneId: new("10.1.1"),
	})
	if err != nil || zones.temp.writeEntity != zones.zone10 || zones.temp.writeValue != 22 {
		t.Fatalf("write to 10.1.1: err=%v entity matches=%v value=%v", err, zones.temp.writeEntity == zones.zone10, zones.temp.writeValue)
	}

	// 28 °C fits zone 2.1.1 (5–30) but not zone 10.1.1 (10–25).
	_, err = zones.svc.SetRoomHeatingTemperature(ctx, &pb.SetRoomHeatingTemperatureRequest{
		Ski: testValidSKI, ValueCelsius: 28, ZoneId: new("10.1.1"),
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("out-of-range write error = %v, want InvalidArgument", err)
	}
	_, err = zones.svc.SetRoomHeatingTemperature(ctx, &pb.SetRoomHeatingTemperatureRequest{
		Ski: testValidSKI, ValueCelsius: 28, ZoneId: new("2.1.1"),
	})
	if err != nil || zones.temp.writeEntity != zones.zone2 {
		t.Fatalf("write to 2.1.1: %v", err)
	}

	_, err = zones.svc.SetRoomHeatingTemperature(ctx, &pb.SetRoomHeatingTemperatureRequest{
		Ski: testValidSKI, ValueCelsius: 22, ZoneId: new("7.1.1"),
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown zone error = %v, want NotFound", err)
	}

	_, err = zones.svc.SetRoomHeatingMode(ctx, &pb.SetRoomHeatingModeRequest{Ski: testValidSKI, Mode: "off"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("mode write without zone_id error = %v, want FailedPrecondition", err)
	}
	_, err = zones.svc.SetRoomHeatingMode(ctx, &pb.SetRoomHeatingModeRequest{Ski: testValidSKI, Mode: "on", ZoneId: new("2.1.1")})
	if err != nil || zones.sysfn.writeEntity != zones.zone2 || zones.sysfn.writeMode != "on" {
		t.Fatalf("mode write to 2.1.1: %v", err)
	}
}

// Before any zone is negotiated, a write naming a zone must not fall back to
// the single-entity lookup, which knows nothing about zones.
func TestHVACServiceNamedZoneWithoutZoneListIsNotFound(t *testing.T) {
	zone := roomEntity(t, testValidSKI, 5, 1, 1)
	temp := &zonedRoomHeatingTemp{
		single: &eebus.EntityResolution{Entity: zone, DeviceCount: 1},
		states: map[spineapi.EntityRemoteInterface]usecases.RoomHeatingSetpoint{
			zone: {Value: 21, Minimum: 5, Maximum: 30, Step: 0.5, Writable: true},
		},
	}
	sysfn := &zonedRoomSysFn{single: &eebus.EntityResolution{Entity: zone, DeviceCount: 1}}
	svc := NewHVACService(temp, sysfn, nil, nil)
	ctx := context.Background()

	_, err := svc.SetRoomHeatingTemperature(ctx, &pb.SetRoomHeatingTemperatureRequest{
		Ski: testValidSKI, ValueCelsius: 22, ZoneId: new("6.1.1"),
	})
	if status.Code(err) != codes.NotFound || temp.writeEntity != nil {
		t.Fatalf("setpoint write to 6.1.1 without zone list: err=%v, reached an entity=%v", err, temp.writeEntity != nil)
	}
	_, err = svc.SetRoomHeatingMode(ctx, &pb.SetRoomHeatingModeRequest{Ski: testValidSKI, Mode: "off", ZoneId: new("6.1.1")})
	if status.Code(err) != codes.NotFound || sysfn.writeEntity != nil {
		t.Fatalf("mode write to 6.1.1 without zone list: err=%v, reached an entity=%v", err, sysfn.writeEntity != nil)
	}
}

func TestHVACServiceSingleZoneNeedsNoZoneID(t *testing.T) {
	zone := roomEntity(t, testValidSKI, 5, 1, 1)
	temp := &zonedRoomHeatingTemp{
		entities: []spineapi.EntityRemoteInterface{zone},
		states: map[spineapi.EntityRemoteInterface]usecases.RoomHeatingSetpoint{
			zone: {Value: 21, Minimum: 5, Maximum: 30, Step: 0.5, Writable: true},
		},
	}
	svc := NewHVACService(temp, nil, nil, nil)

	if _, err := svc.SetRoomHeatingTemperature(context.Background(), &pb.SetRoomHeatingTemperatureRequest{
		Ski: testValidSKI, ValueCelsius: 22,
	}); err != nil || temp.writeEntity != zone {
		t.Fatalf("single-zone write without zone_id: %v", err)
	}
}

func TestHVACServiceZoneEventsCarryTheirZoneState(t *testing.T) {
	zones := newTwoZones(t)

	event := &pb.RoomHeatingEvent{
		Ski:       testValidSKI,
		EventType: pb.RoomHeatingEventType_ROOM_HEATING_EVENT_SETPOINT_UPDATED,
		ZoneId:    new("10.1.1"),
	}
	if !zones.svc.AttachRoomHeatingPayload(event, testValidSKI) {
		t.Fatal("zone event payload not readable")
	}
	if event.GetState().GetZoneLabel() != "Zone 2" || event.GetState().GetSetpoint().GetValueCelsius() != 23 {
		t.Fatalf("event state = %v, want zone 10.1.1", event.GetState())
	}

	// The consolidated stream passes the zone on.
	heating := roomHeatingEvent(eebus.Event{
		SKI: testValidSKI, Type: eebus.EventTypeRoomHeatingSetpointUpdated, ZoneID: "10.1.1",
	})
	if heating.GetZoneId() != "10.1.1" {
		t.Fatalf("consolidated event zone = %q", heating.GetZoneId())
	}
}
