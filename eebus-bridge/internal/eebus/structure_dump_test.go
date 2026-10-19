package eebus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	spineapi "github.com/enbility/spine-go/api"
	"github.com/enbility/spine-go/mocks"
	"github.com/enbility/spine-go/model"
	"github.com/enbility/spine-go/util"
	"github.com/stretchr/testify/mock"
)

const dumpTestSKI = "0123456789abcdef0123456789abcdef01234567"

func readableOperations(t *testing.T, write bool) *mocks.OperationsInterface {
	t.Helper()
	ops := mocks.NewOperationsInterface(t)
	ops.On("Read").Return(true).Maybe()
	ops.On("ReadPartial").Return(false).Maybe()
	ops.On("Write").Return(write).Maybe()
	ops.On("WritePartial").Return(false).Maybe()
	return ops
}

func notReadableOperations(t *testing.T) *mocks.OperationsInterface {
	t.Helper()
	ops := mocks.NewOperationsInterface(t)
	ops.On("Read").Return(false).Maybe()
	ops.On("ReadPartial").Return(false).Maybe()
	ops.On("Write").Return(false).Maybe()
	ops.On("WritePartial").Return(false).Maybe()
	return ops
}

func remoteFeature(
	t *testing.T,
	entity []model.AddressEntityType,
	number uint,
	featureType model.FeatureTypeType,
	role model.RoleType,
	operations map[model.FunctionType]spineapi.OperationsInterface,
	data map[model.FunctionType]any,
) *mocks.FeatureRemoteInterface {
	t.Helper()
	feature := mocks.NewFeatureRemoteInterface(t)
	featureNumber := model.AddressFeatureType(number)
	feature.On("Address").Return(&model.FeatureAddressType{Entity: entity, Feature: &featureNumber}).Maybe()
	feature.On("Type").Return(featureType).Maybe()
	feature.On("Role").Return(role).Maybe()
	feature.On("Description").Return((*model.DescriptionType)(nil)).Maybe()
	// testify formats call arguments, which invokes String() on the mock.
	feature.On("String").Return(fmt.Sprintf("feature %v/%d", entity, number)).Maybe()
	feature.On("Operations").Return(operations).Maybe()
	feature.On("DataCopy", mock.Anything).Return(func(function model.FunctionType) any { return data[function] }).Maybe()
	return feature
}

func remoteEntity(
	t *testing.T,
	address []model.AddressEntityType,
	entityType model.EntityTypeType,
	features ...spineapi.FeatureRemoteInterface,
) *mocks.EntityRemoteInterface {
	t.Helper()
	entity := mocks.NewEntityRemoteInterface(t)
	entity.On("Address").Return(&model.EntityAddressType{Entity: address}).Maybe()
	entity.On("EntityType").Return(entityType).Maybe()
	entity.On("Features").Return(features).Maybe()
	return entity
}

func remoteDevice(t *testing.T, entities ...spineapi.EntityRemoteInterface) *mocks.DeviceRemoteInterface {
	t.Helper()
	deviceType := model.DeviceTypeTypeHeatgenerationSystem
	address := model.AddressDeviceType("d:_i:47859_VR940")
	actor := model.UseCaseActorTypeMonitoredUnit
	name := model.UseCaseNameTypeMonitoringOfPowerConsumption
	version := model.SpecificationVersionType("1.0.0")
	device := mocks.NewDeviceRemoteInterface(t)
	device.On("Ski").Return(dumpTestSKI).Maybe()
	device.On("DeviceType").Return(&deviceType).Maybe()
	device.On("Address").Return(&address).Maybe()
	device.On("Entities").Return(entities).Maybe()
	device.On("UseCases").Return([]model.UseCaseInformationDataType{{
		Actor: &actor,
		UseCaseSupport: []model.UseCaseSupportType{{
			UseCaseName:      &name,
			UseCaseVersion:   &version,
			UseCaseAvailable: util.Ptr(true),
			ScenarioSupport:  []model.UseCaseScenarioSupportType{3, 1, 2},
		}},
	}}).Maybe()
	return device
}

// localDeviceWithClients returns a local device whose only entity owns the given
// client features. Strict mocks: any call not set up here (for example a write
// or adding a feature) fails the test.
func localDeviceWithClients(
	t *testing.T,
	remote spineapi.DeviceRemoteInterface,
	clients map[model.FeatureTypeType]spineapi.FeatureLocalInterface,
) *mocks.DeviceLocalInterface {
	t.Helper()
	entity := mocks.NewEntityLocalInterface(t)
	entity.On("FeatureOfTypeAndRole", mock.Anything, model.RoleTypeClient).Return(
		func(featureType model.FeatureTypeType, _ model.RoleType) spineapi.FeatureLocalInterface {
			if client, ok := clients[featureType]; ok {
				return client
			}
			return nil
		}).Maybe()
	local := mocks.NewDeviceLocalInterface(t)
	local.On("RemoteDevices").Return([]spineapi.DeviceRemoteInterface{remote}).Maybe()
	local.On("Entities").Return([]spineapi.EntityLocalInterface{entity}).Maybe()
	return local
}

// answeringClient answers every read with the given response data.
func answeringClient(t *testing.T, response any) *mocks.FeatureLocalInterface {
	t.Helper()
	client := mocks.NewFeatureLocalInterface(t)
	counter := model.MsgCounterType(0)
	client.On("RequestRemoteData", mock.Anything, nil, nil, mock.Anything).Return(
		func(model.FunctionType, any, any, spineapi.FeatureRemoteInterface) *model.MsgCounterType {
			counter++
			return util.Ptr(counter)
		}, (*model.ErrorType)(nil))
	client.On("AddResponseCallback", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		callback := args.Get(1).(func(spineapi.ResponseMessage))
		callback(spineapi.ResponseMessage{MsgCounterReference: args.Get(0).(model.MsgCounterType), Data: response})
	}).Return(nil)
	return client
}

func newTestDumper(local spineapi.DeviceLocalInterface, readWait time.Duration) *StructureDumper {
	dumper := NewStructureDumper(func() spineapi.DeviceLocalInterface { return local }, readWait)
	dumper.now = func() time.Time { return time.Date(2026, 10, 19, 8, 0, 0, 0, time.UTC) }
	return dumper
}

func setpointServer(t *testing.T, entity []model.AddressEntityType, setpoint float64) *mocks.FeatureRemoteInterface {
	t.Helper()
	return remoteFeature(t, entity, 3, model.FeatureTypeTypeSetpoint, model.RoleTypeServer,
		map[model.FunctionType]spineapi.OperationsInterface{
			model.FunctionTypeSetpointListData:            readableOperations(t, true),
			model.FunctionTypeSetpointConstraintsListData: readableOperations(t, false),
		},
		map[model.FunctionType]any{
			model.FunctionTypeSetpointListData: &model.SetpointListDataType{SetpointData: []model.SetpointDataType{{
				SetpointId: util.Ptr(model.SetpointIdType(1)),
				Value:      model.NewScaledNumberType(setpoint),
			}}},
		})
}

func TestStructureDumpReadsBothZonesInNumericAddressOrder(t *testing.T) {
	zone10 := remoteEntity(t, []model.AddressEntityType{10}, model.EntityTypeTypeHvacRoom, setpointServer(t, []model.AddressEntityType{10}, 19.5))
	zone2 := remoteEntity(t, []model.AddressEntityType{2}, model.EntityTypeTypeHvacRoom, setpointServer(t, []model.AddressEntityType{2}, 21))
	remote := remoteDevice(t, zone10, zone2)
	client := answeringClient(t, &model.SetpointListDataType{})
	local := localDeviceWithClients(t, remote, map[model.FeatureTypeType]spineapi.FeatureLocalInterface{
		model.FeatureTypeTypeSetpoint: client,
	})

	structure, err := newTestDumper(local, time.Second).Dump(context.Background(), dumpTestSKI)
	if err != nil {
		t.Fatal(err)
	}

	if got := len(structure.Entities); got != 2 {
		t.Fatalf("entities = %d, want 2", got)
	}
	if structure.Entities[0].Address != "2" || structure.Entities[1].Address != "10" {
		t.Fatalf("entity order = %s, %s; want 2, 10", structure.Entities[0].Address, structure.Entities[1].Address)
	}
	for _, entity := range structure.Entities {
		functions := entity.Features[0].Functions
		if len(functions) != 2 {
			t.Fatalf("entity %s functions = %d, want 2", entity.Address, len(functions))
		}
		for _, function := range functions {
			if function.ReadStatus != ReadStatusRead {
				t.Errorf("entity %s %s read_status = %q, want %q", entity.Address, function.Name, function.ReadStatus, ReadStatusRead)
			}
		}
	}
	client.AssertNumberOfCalls(t, "RequestRemoteData", 4)

	if structure.SKI != ShortSKI(dumpTestSKI) {
		t.Errorf("ski = %q, want shortened %q", structure.SKI, ShortSKI(dumpTestSKI))
	}
	if got := structure.UseCases[0].Scenarios; len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Errorf("scenarios = %v, want sorted [1 2 3]", got)
	}
}

func TestStructureDumpEncodesCachedSPINEDataAsJSON(t *testing.T) {
	zone := remoteEntity(t, []model.AddressEntityType{2, 1}, model.EntityTypeTypeHvacRoom, setpointServer(t, []model.AddressEntityType{2, 1}, 21))
	remote := remoteDevice(t, zone)
	local := localDeviceWithClients(t, remote, map[model.FeatureTypeType]spineapi.FeatureLocalInterface{
		model.FeatureTypeTypeSetpoint: answeringClient(t, &model.SetpointListDataType{}),
	})

	structure, err := newTestDumper(local, time.Second).Dump(context.Background(), dumpTestSKI)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(structure)
	if err != nil {
		t.Fatal(err)
	}

	var decoded struct {
		Entities []struct {
			Address  string `json:"address"`
			Features []struct {
				Address   string `json:"address"`
				Functions []struct {
					Name       string          `json:"name"`
					Operations string          `json:"operations"`
					Data       json.RawMessage `json:"data"`
				} `json:"functions"`
			} `json:"features"`
		} `json:"entities"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	feature := decoded.Entities[0].Features[0]
	if decoded.Entities[0].Address != "2:1" || feature.Address != "2:1:3" {
		t.Fatalf("addresses = %s / %s, want 2:1 / 2:1:3", decoded.Entities[0].Address, feature.Address)
	}
	listData := feature.Functions[1]
	if listData.Name != string(model.FunctionTypeSetpointListData) || listData.Operations != "r,w" {
		t.Fatalf("function = %s (%s), want setpointListData (r,w)", listData.Name, listData.Operations)
	}
	if len(listData.Data) == 0 || string(listData.Data) == "null" {
		t.Fatal("setpointListData has no cached data in the JSON dump")
	}
}

func TestStructureDumpReportsFunctionsItCannotRead(t *testing.T) {
	measurement := remoteFeature(t, []model.AddressEntityType{1}, 1, model.FeatureTypeTypeMeasurement, model.RoleTypeServer,
		map[model.FunctionType]spineapi.OperationsInterface{
			model.FunctionTypeMeasurementListData: readableOperations(t, false),
		}, nil)
	writeOnly := remoteFeature(t, []model.AddressEntityType{1}, 2, model.FeatureTypeTypeSetpoint, model.RoleTypeServer,
		map[model.FunctionType]spineapi.OperationsInterface{
			model.FunctionTypeSetpointListData: notReadableOperations(t),
		}, nil)
	client := remoteFeature(t, []model.AddressEntityType{1}, 3, model.FeatureTypeTypeLoadControl, model.RoleTypeClient,
		map[model.FunctionType]spineapi.OperationsInterface{}, nil)
	remote := remoteDevice(t, remoteEntity(t, []model.AddressEntityType{1}, model.EntityTypeTypeHeatPumpAppliance, measurement, writeOnly, client))
	// No local Measurement client exists, and the Setpoint client must not be used
	// for a function the device does not permit reading.
	local := localDeviceWithClients(t, remote, map[model.FeatureTypeType]spineapi.FeatureLocalInterface{
		model.FeatureTypeTypeSetpoint: mocks.NewFeatureLocalInterface(t),
	})

	structure, err := newTestDumper(local, time.Second).Dump(context.Background(), dumpTestSKI)
	if err != nil {
		t.Fatal(err)
	}

	features := structure.Entities[0].Features
	if got := features[0].Functions[0].ReadStatus; got != ReadStatusNoLocalClient {
		t.Errorf("measurement read_status = %q, want %q", got, ReadStatusNoLocalClient)
	}
	if got := features[1].Functions[0].ReadStatus; got != ReadStatusNotReadable {
		t.Errorf("write-only setpoint read_status = %q, want %q", got, ReadStatusNotReadable)
	}
	if features[2].Role != string(model.RoleTypeClient) || len(features[2].Functions) != 0 {
		t.Errorf("client feature = %+v, want role client without functions", features[2])
	}
}

func TestStructureDumpMarksUnansweredAndRejectedReads(t *testing.T) {
	silent := mocks.NewFeatureLocalInterface(t)
	silent.On("RequestRemoteData", mock.Anything, nil, nil, mock.Anything).Return(util.Ptr(model.MsgCounterType(1)), (*model.ErrorType)(nil))
	silent.On("AddResponseCallback", mock.Anything, mock.Anything).Return(nil)
	rejected := model.ResultDataType{
		ErrorNumber: util.Ptr(model.ErrorNumberTypeCommandRejected),
		Description: util.Ptr(model.DescriptionType("not allowed")),
	}
	measurement := remoteFeature(t, []model.AddressEntityType{1}, 1, model.FeatureTypeTypeMeasurement, model.RoleTypeServer,
		map[model.FunctionType]spineapi.OperationsInterface{
			model.FunctionTypeMeasurementListData: readableOperations(t, false),
		}, nil)
	setpoint := remoteFeature(t, []model.AddressEntityType{1}, 2, model.FeatureTypeTypeSetpoint, model.RoleTypeServer,
		map[model.FunctionType]spineapi.OperationsInterface{
			model.FunctionTypeSetpointListData: readableOperations(t, false),
		}, nil)
	remote := remoteDevice(t, remoteEntity(t, []model.AddressEntityType{1}, model.EntityTypeTypeHeatPumpAppliance, measurement, setpoint))
	local := localDeviceWithClients(t, remote, map[model.FeatureTypeType]spineapi.FeatureLocalInterface{
		model.FeatureTypeTypeMeasurement: silent,
		model.FeatureTypeTypeSetpoint:    answeringClient(t, &rejected),
	})

	start := time.Now()
	structure, err := newTestDumper(local, 50*time.Millisecond).Dump(context.Background(), dumpTestSKI)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("dump took %s, want it bounded by the read wait", elapsed)
	}

	features := structure.Entities[0].Features
	if got := features[0].Functions[0].ReadStatus; got != ReadStatusNoResponse {
		t.Errorf("silent read_status = %q, want %q", got, ReadStatusNoResponse)
	}
	rejectedFunction := features[1].Functions[0]
	if rejectedFunction.ReadStatus != ReadStatusFailed || rejectedFunction.ReadError == "" {
		t.Errorf("rejected function = %+v, want failed with an error text", rejectedFunction)
	}
}

func TestStructureDumpFailsWithoutConnectedDevice(t *testing.T) {
	local := mocks.NewDeviceLocalInterface(t)
	local.On("RemoteDevices").Return([]spineapi.DeviceRemoteInterface{})

	_, err := newTestDumper(local, time.Second).Dump(context.Background(), dumpTestSKI)
	if !errors.Is(err, ErrRemoteDeviceNotConnected) {
		t.Fatalf("err = %v, want ErrRemoteDeviceNotConnected", err)
	}

	_, err = newTestDumper(nil, time.Second).Dump(context.Background(), dumpTestSKI)
	if !errors.Is(err, ErrRemoteDeviceNotConnected) {
		t.Fatalf("err without local device = %v, want ErrRemoteDeviceNotConnected", err)
	}
}
