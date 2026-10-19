package grpc

import (
	"context"
	"slices"

	spineapi "github.com/enbility/spine-go/api"
	pb "github.com/volschin/eebus-bridge/gen/proto/eebus/v1"
	"github.com/volschin/eebus-bridge/internal/eebus"
	"github.com/volschin/eebus-bridge/internal/usecases"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type roomHeatingTempController interface {
	CompatibleEntity(string) eebus.EntityResolution
	State(spineapi.EntityRemoteInterface) (usecases.RoomHeatingSetpoint, error)
	Write(context.Context, spineapi.EntityRemoteInterface, float64) error
}

type roomHeatingSysFnController interface {
	CompatibleEntity(string) eebus.EntityResolution
	State(spineapi.EntityRemoteInterface) (usecases.RoomHeatingSystemFunctionState, error)
	WriteOperationMode(context.Context, spineapi.EntityRemoteInterface, string) error
}

// roomHeatingZoneLister lists every room entity of a device, one per heating
// zone, in address order. The use cases implement it; a source without it
// serves the device as a single zone.
type roomHeatingZoneLister interface {
	CompatibleEntities(string) []spineapi.EntityRemoteInterface
}

// roomTemperatureZoneReader reads the room temperature of one zone's entity.
type roomTemperatureZoneReader interface {
	roomHeatingZoneLister
	EntityTemperature(spineapi.EntityRemoteInterface) (float64, error)
}

// HVACService exposes the room-heating Configuration use cases over gRPC.
type HVACService struct {
	pb.UnimplementedHVACServiceServer
	temp     roomHeatingTempController
	sysfn    roomHeatingSysFnController
	room     temperatureReader
	bus      *eebus.EventBus
	registry *eebus.DeviceRegistry
}

func NewHVACService(
	temp roomHeatingTempController,
	sysfn roomHeatingSysFnController,
	room temperatureReader,
	bus *eebus.EventBus,
	registries ...*eebus.DeviceRegistry,
) *HVACService {
	var registry *eebus.DeviceRegistry
	if len(registries) > 0 {
		registry = registries[0]
	}
	return &HVACService{temp: temp, sysfn: sysfn, room: room, bus: bus, registry: registry}
}

func (s *HVACService) GetRoomHeating(_ context.Context, req *pb.DeviceRequest) (*pb.RoomHeatingState, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if _, err := normalizeReadSKI(req.Ski); err != nil {
		return nil, err
	}
	return s.snapshotRoomHeating(req.Ski)
}

func (s *HVACService) ListRoomHeatingZones(_ context.Context, req *pb.DeviceRequest) (*pb.RoomHeatingZoneList, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if _, err := normalizeReadSKI(req.Ski); err != nil {
		return nil, err
	}
	zones, listed, err := s.roomHeatingZones(req.Ski)
	if !listed {
		return nil, status.Error(codes.Unimplemented, "room heating sources cannot list zones")
	}
	if err != nil {
		return nil, err
	}
	return &pb.RoomHeatingZoneList{Zones: zones}, nil
}

// RoomHeatingZones returns every readable zone for the device snapshot, or
// nil when the zones are not readable.
func (s *HVACService) RoomHeatingZones(ski string) []*pb.RoomHeatingZone {
	zones, _, err := s.roomHeatingZones(ski)
	if err != nil {
		return nil
	}
	return zones
}

// snapshotRoomHeating serves single-zone clients: the first zone in address
// order, its values and label always from the same zone.
func (s *HVACService) snapshotRoomHeating(ski string) (*pb.RoomHeatingState, error) {
	zones, listed, err := s.roomHeatingZones(ski)
	if !listed {
		return s.deviceRoomHeating(ski)
	}
	if err != nil {
		return nil, err
	}
	return zones[0].GetState(), nil
}

// zoneEntities holds one zone's entity per use case; nil where a use case does
// not cover the zone.
type zoneEntities struct {
	id    string
	order spineapi.EntityRemoteInterface
	temp  spineapi.EntityRemoteInterface
	sysfn spineapi.EntityRemoteInterface
	room  spineapi.EntityRemoteInterface
}

// listsZones reports whether the room-heating sources can list zones.
func (s *HVACService) listsZones() bool {
	if s.temp == nil && s.sysfn == nil {
		return false
	}
	if _, ok := s.temp.(roomHeatingZoneLister); s.temp != nil && !ok {
		return false
	}
	if _, ok := s.sysfn.(roomHeatingZoneLister); s.sysfn != nil && !ok {
		return false
	}
	return true
}

// roomHeatingZones reads every zone of the device in address order. listed is
// false when the sources cannot list zones. Zones without any readable value
// are left out; none readable at all is UNAVAILABLE.
func (s *HVACService) roomHeatingZones(ski string) (zones []*pb.RoomHeatingZone, listed bool, err error) {
	if !s.listsZones() {
		return nil, false, nil
	}
	deviceSKI, err := s.zoneDeviceSKI(ski)
	if err != nil {
		return nil, true, err
	}
	entities := s.zoneEntities(deviceSKI)
	if len(entities) == 0 {
		if s.registry != nil {
			s.registry.RecordCapabilityMissingEntity(deviceSKI, eebus.CapabilityRoomHeating)
		}
		return nil, true, status.Errorf(codes.NotFound, "no compatible HVACRoom found for %s ski", requestedSKIForError(ski))
	}
	for _, zone := range entities {
		if state, read := s.readZone(deviceSKI, zone); read {
			zones = append(zones, &pb.RoomHeatingZone{ZoneId: zone.id, State: state})
		}
	}
	if len(zones) == 0 {
		if s.registry != nil {
			s.registry.RecordCapabilityRead(deviceSKI, eebus.CapabilityRoomHeating, usecases.ErrRoomHeatingDataUnavailable)
		}
		return nil, true, status.Error(codes.Unavailable, "reading room heating: temporarily unavailable")
	}
	if s.registry != nil {
		s.registry.RecordCapabilityRead(deviceSKI, eebus.CapabilityRoomHeating, nil)
	}
	return zones, true, nil
}

// zoneDeviceSKI resolves an empty SKI to the only compatible device, so the
// zones of several devices are never mixed.
func (s *HVACService) zoneDeviceSKI(ski string) (string, error) {
	if eebus.NormalizeSKI(ski) != "" {
		return ski, nil
	}
	var resolve func(string) eebus.EntityResolution
	if s.temp != nil {
		resolve = s.temp.CompatibleEntity
	} else {
		resolve = s.sysfn.CompatibleEntity
	}
	entity, err := resolveCompatibleEntity(ski, "HVACRoom", eebus.CapabilityRoomHeating, s.registry, resolve)
	if err != nil {
		return "", err
	}
	if entity.Device() == nil {
		return "", status.Error(codes.Unavailable, "room heating entity has no device")
	}
	return entity.Device().Ski(), nil
}

// zoneEntities collects each zone's entities from all use cases, keyed by
// zone ID, in address order.
func (s *HVACService) zoneEntities(ski string) []*zoneEntities {
	byID := make(map[string]*zoneEntities)
	var zones []*zoneEntities
	add := func(source any, assign func(*zoneEntities, spineapi.EntityRemoteInterface)) {
		lister, ok := source.(roomHeatingZoneLister)
		if !ok {
			return
		}
		for _, entity := range lister.CompatibleEntities(ski) {
			id := eebus.ZoneID(entity)
			zone := byID[id]
			if zone == nil {
				zone = &zoneEntities{id: id, order: entity}
				byID[id] = zone
				zones = append(zones, zone)
			}
			assign(zone, entity)
		}
	}
	if s.temp != nil {
		add(s.temp, func(zone *zoneEntities, entity spineapi.EntityRemoteInterface) { zone.temp = entity })
	}
	if s.sysfn != nil {
		add(s.sysfn, func(zone *zoneEntities, entity spineapi.EntityRemoteInterface) { zone.sysfn = entity })
	}
	if s.room != nil {
		add(s.room, func(zone *zoneEntities, entity spineapi.EntityRemoteInterface) { zone.room = entity })
	}
	slices.SortStableFunc(zones, func(a, b *zoneEntities) int { return eebus.CompareEntities(a.order, b.order) })
	return zones
}

// readZone reads one zone's values. read is false when nothing was readable;
// the label alone, cached device metadata, never makes a zone readable.
func (s *HVACService) readZone(ski string, zone *zoneEntities) (state *pb.RoomHeatingState, read bool) {
	state = &pb.RoomHeatingState{}
	if s.registry != nil {
		if label := s.registry.ZoneLabel(ski, zone.id); label != "" {
			state.ZoneLabel = new(label)
		}
	}
	if reader, ok := s.room.(roomTemperatureZoneReader); ok && zone.room != nil {
		if value, err := reader.EntityTemperature(zone.room); err == nil {
			state.CurrentTemperatureCelsius = &value
			read = true
		}
	}
	if zone.temp != nil {
		if setpoint, err := s.temp.State(zone.temp); err == nil {
			state.Setpoint = convertRoomHeatingSetpoint(setpoint)
			read = true
		}
	}
	if zone.sysfn != nil {
		if sysfn, err := s.sysfn.State(zone.sysfn); err == nil {
			state.SystemFunction = convertRoomHeatingSystemFunction(sysfn)
			read = true
		}
	}
	return state, read
}

// deviceRoomHeating serves sources that cannot list zones: one aggregate per
// device.
func (s *HVACService) deviceRoomHeating(ski string) (*pb.RoomHeatingState, error) {
	state := &pb.RoomHeatingState{}
	entityResolved := false
	readSucceeded := false
	var resolveErr error
	// The zone label is device metadata cached by DeviceClassifier, not a live
	// room-heating read. It deliberately does not set readSucceeded: a label on
	// its own must never make an otherwise unavailable aggregate look readable.
	if s.registry != nil {
		if label := s.registry.ZoneLabel(ski, ""); label != "" {
			state.ZoneLabel = new(label)
		}
	}
	if s.room != nil {
		if value, err := s.room.Temperature(ski); err == nil {
			state.CurrentTemperatureCelsius = &value
			readSucceeded = true
		}
	}
	if s.temp != nil {
		entity, err := resolveCompatibleEntity(
			ski,
			"HVACRoom",
			eebus.CapabilityRoomHeating,
			s.registry,
			s.temp.CompatibleEntity,
		)
		if err == nil {
			entityResolved = true
			if setpoint, err := s.temp.State(entity); err == nil {
				state.Setpoint = convertRoomHeatingSetpoint(setpoint)
				readSucceeded = true
			}
		} else if status.Code(err) == codes.InvalidArgument || status.Code(err) == codes.FailedPrecondition {
			return nil, err
		} else if resolveErr == nil {
			resolveErr = err
		}
	}
	if s.sysfn != nil {
		entity, err := resolveCompatibleEntity(
			ski,
			"HVACRoom",
			eebus.CapabilityRoomHeating,
			s.registry,
			s.sysfn.CompatibleEntity,
		)
		if err == nil {
			entityResolved = true
			if sysfn, err := s.sysfn.State(entity); err == nil {
				state.SystemFunction = convertRoomHeatingSystemFunction(sysfn)
				readSucceeded = true
			}
		} else if status.Code(err) == codes.InvalidArgument || status.Code(err) == codes.FailedPrecondition {
			return nil, err
		} else if resolveErr == nil {
			resolveErr = err
		}
	}
	if !entityResolved && !readSucceeded {
		if resolveErr != nil {
			return nil, resolveErr
		}
		if s.registry != nil {
			s.registry.RecordCapabilityMissingEntity(ski, eebus.CapabilityRoomHeating)
		}
		return nil, status.Errorf(codes.NotFound, "no compatible HVACRoom found for %s ski", requestedSKIForError(ski))
	}
	if !readSucceeded {
		if s.registry != nil {
			s.registry.RecordCapabilityRead(ski, eebus.CapabilityRoomHeating, usecases.ErrRoomHeatingDataUnavailable)
		}
		return nil, status.Error(codes.Unavailable, "reading room heating: temporarily unavailable")
	}
	if s.registry != nil {
		s.registry.RecordCapabilityRead(ski, eebus.CapabilityRoomHeating, nil)
	}
	return state, nil
}

// zoneWriteEntity picks the entity a write goes to. Without zone_id only a
// device with exactly one zone qualifies: a write never guesses between zones.
func (s *HVACService) zoneWriteEntity(
	ski string,
	zoneID *string,
	source any,
	resolve func(string) eebus.EntityResolution,
) (spineapi.EntityRemoteInterface, error) {
	lister, ok := source.(roomHeatingZoneLister)
	if !ok {
		if zoneID != nil {
			return nil, status.Error(codes.Unimplemented, "room heating source cannot address zones")
		}
		return resolveCompatibleEntity(ski, "HVACRoom", eebus.CapabilityRoomHeating, s.registry, resolve)
	}
	entities := lister.CompatibleEntities(ski)
	if len(entities) == 0 {
		// A named zone is only ever looked up in the zone list; the single-entity
		// path could resolve an entity of another zone.
		if zoneID != nil && *zoneID != "" {
			return nil, status.Errorf(codes.NotFound, "unknown heating zone %q", *zoneID)
		}
		return resolveCompatibleEntity(ski, "HVACRoom", eebus.CapabilityRoomHeating, s.registry, resolve)
	}
	if zoneID == nil || *zoneID == "" {
		if len(entities) > 1 {
			return nil, status.Errorf(
				codes.FailedPrecondition,
				"device has %d heating zones; zone_id is required",
				len(entities),
			)
		}
		return entities[0], nil
	}
	for _, entity := range entities {
		if eebus.ZoneID(entity) == *zoneID {
			return entity, nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "unknown heating zone %q", *zoneID)
}

func (s *HVACService) SetRoomHeatingTemperature(
	ctx context.Context,
	req *pb.SetRoomHeatingTemperatureRequest,
) (*pb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := requireWriteSKI(req.Ski); err != nil {
		return nil, err
	}
	if s.temp == nil {
		return nil, status.Error(codes.Unavailable, "room heating temperature use case not initialized")
	}
	entity, err := s.zoneWriteEntity(req.Ski, req.ZoneId, s.temp, s.temp.CompatibleEntity)
	if err != nil {
		return nil, err
	}
	// The write validates against this zone's own limits and step.
	if err := s.temp.Write(ctx, entity, req.ValueCelsius); err != nil {
		return nil, mapRoomHeatingError("writing room heating setpoint", err)
	}
	return &pb.Empty{}, nil
}

func (s *HVACService) SetRoomHeatingMode(
	ctx context.Context,
	req *pb.SetRoomHeatingModeRequest,
) (*pb.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := requireWriteSKI(req.Ski); err != nil {
		return nil, err
	}
	if s.sysfn == nil {
		return nil, status.Error(codes.Unavailable, "room heating system function use case not initialized")
	}
	entity, err := s.zoneWriteEntity(req.Ski, req.ZoneId, s.sysfn, s.sysfn.CompatibleEntity)
	if err != nil {
		return nil, err
	}
	if err := s.sysfn.WriteOperationMode(ctx, entity, req.Mode); err != nil {
		return nil, mapRoomHeatingError("writing room heating mode", err)
	}
	return &pb.Empty{}, nil
}

func (s *HVACService) SubscribeRoomHeatingEvents(
	req *pb.DeviceRequest,
	stream pb.HVACService_SubscribeRoomHeatingEventsServer,
) error {
	return subscribeFilteredEvents(s.bus, req, stream.Context(), stream.Send, func(evt eebus.Event) (*pb.RoomHeatingEvent, bool) {
		var eventType pb.RoomHeatingEventType
		switch evt.Type {
		case eebus.EventTypeRoomHeatingUseCaseSupportUpdated, eebus.EventTypeRoomHeatingSystemFunctionSupportUpdated:
			eventType = pb.RoomHeatingEventType_ROOM_HEATING_EVENT_SUPPORT_UPDATED
		case eebus.EventTypeRoomTemperatureUpdated:
			eventType = pb.RoomHeatingEventType_ROOM_HEATING_EVENT_CURRENT_TEMPERATURE_UPDATED
		case eebus.EventTypeRoomHeatingSetpointUpdated:
			eventType = pb.RoomHeatingEventType_ROOM_HEATING_EVENT_SETPOINT_UPDATED
		case eebus.EventTypeRoomHeatingSystemFunctionUpdated:
			eventType = pb.RoomHeatingEventType_ROOM_HEATING_EVENT_SYSTEM_FUNCTION_UPDATED
		default:
			return nil, false
		}
		event := &pb.RoomHeatingEvent{Ski: evt.SKI, EventType: eventType}
		if evt.ZoneID != "" {
			event.ZoneId = new(evt.ZoneID)
		}
		s.attachRoomHeatingPayload(event, evt.SKI)
		return event, true
	})
}

// attachRoomHeatingPayload keeps the legacy-stream contract (attach whatever
// partial aggregate reads cleanly) and reports whether the event's specific
// target field was readable; only the consolidated envelope's availability is
// derived from that stricter answer. An event naming a zone carries that
// zone's state.
func (s *HVACService) attachRoomHeatingPayload(event *pb.RoomHeatingEvent, ski string) bool {
	state, err := s.roomHeatingState(ski, event.GetZoneId())
	if err != nil {
		return false
	}
	event.State = state
	switch event.GetEventType() {
	case pb.RoomHeatingEventType_ROOM_HEATING_EVENT_CURRENT_TEMPERATURE_UPDATED:
		return state.CurrentTemperatureCelsius != nil
	case pb.RoomHeatingEventType_ROOM_HEATING_EVENT_SETPOINT_UPDATED:
		return state.Setpoint != nil
	case pb.RoomHeatingEventType_ROOM_HEATING_EVENT_SYSTEM_FUNCTION_UPDATED:
		return state.SystemFunction != nil
	}
	return false
}

// roomHeatingState is the state of the zone with zoneID, or the single-zone
// aggregate when no zone is named or the sources cannot list zones.
func (s *HVACService) roomHeatingState(ski, zoneID string) (*pb.RoomHeatingState, error) {
	if zoneID == "" {
		return s.snapshotRoomHeating(ski)
	}
	zones, listed, err := s.roomHeatingZones(ski)
	if !listed {
		return s.deviceRoomHeating(ski)
	}
	if err != nil {
		return nil, err
	}
	for _, zone := range zones {
		if zone.GetZoneId() == zoneID {
			return zone.GetState(), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "unknown heating zone %q", zoneID)
}

func (s *HVACService) AttachRoomHeatingPayload(event *pb.RoomHeatingEvent, ski string) bool {
	return s.attachRoomHeatingPayload(event, ski)
}

func convertRoomHeatingSetpoint(state usecases.RoomHeatingSetpoint) *pb.RoomHeatingSetpoint {
	return &pb.RoomHeatingSetpoint{
		ValueCelsius: state.Value,
		MinCelsius:   state.Minimum,
		MaxCelsius:   state.Maximum,
		StepCelsius:  state.Step,
		Writable:     state.Writable,
	}
}

func convertRoomHeatingSystemFunction(state usecases.RoomHeatingSystemFunctionState) *pb.RoomHeatingSystemFunction {
	return &pb.RoomHeatingSystemFunction{
		OperationMode:  state.OperationMode,
		AvailableModes: state.AvailableModes,
		ModeWritable:   state.ModeWritable,
	}
}

var roomHeatingErrorClasses = usecaseErrorClasses{
	invalidArgument:    []error{usecases.ErrRoomHeatingOutOfRange, usecases.ErrRoomHeatingInvalidStep, usecases.ErrRoomHeatingSysFnInvalidMode},
	failedPrecondition: []error{usecases.ErrRoomHeatingNotWritable, usecases.ErrRoomHeatingRejected, usecases.ErrRoomHeatingSysFnNotWritable, usecases.ErrRoomHeatingSysFnRejected},
	unavailable:        []error{usecases.ErrRoomHeatingDataUnavailable, usecases.ErrRoomHeatingSysFnDataUnavailable},
}

func mapRoomHeatingError(action string, err error) error {
	return mapUsecaseError(action, err, roomHeatingErrorClasses)
}
