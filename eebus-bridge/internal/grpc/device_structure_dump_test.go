package grpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	pb "github.com/volschin/eebus-bridge/gen/proto/eebus/v1"
	"github.com/volschin/eebus-bridge/internal/eebus"
	bridgegrpc "github.com/volschin/eebus-bridge/internal/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const dumpSKI = "0123456789ABCDEF0123456789ABCDEF01234567"

type fakeStructureDumps struct {
	structure *eebus.DeviceStructure
	err       error
	calls     []string
}

func (f *fakeStructureDumps) Dump(_ context.Context, ski string) (*eebus.DeviceStructure, error) {
	f.calls = append(f.calls, ski)
	return f.structure, f.err
}

func newDumpService(t *testing.T, dumps bridgegrpc.StructureDumpSource) *bridgegrpc.DeviceService {
	t.Helper()
	bus := eebus.NewEventBus()
	registry := eebus.NewDeviceRegistry()
	registry.AddDevice(dumpSKI, eebus.DeviceInfo{})
	registry.MarkConnected(dumpSKI)
	opts := []bridgegrpc.DeviceServiceOption{}
	if dumps != nil {
		opts = append(opts, bridgegrpc.WithStructureDumps(dumps))
	}
	return bridgegrpc.NewDeviceService(eebus.NewCallbacks(bus, false), bus, "local", registry, &recordingTrustController{}, opts...)
}

func TestGetDeviceStructureDumpReturnsJSON(t *testing.T) {
	capturedAt := time.Date(2026, 10, 19, 8, 0, 0, 0, time.UTC)
	dumps := &fakeStructureDumps{structure: &eebus.DeviceStructure{
		SKI:        eebus.ShortSKI(dumpSKI),
		CapturedAt: capturedAt,
		Entities:   []eebus.EntityStructure{{Address: "2:1", Type: "HVACRoom", Features: []eebus.FeatureStructure{}}},
		UseCases:   []eebus.UseCaseStructure{},
	}}
	service := newDumpService(t, dumps)

	dump, err := service.GetDeviceStructureDump(context.Background(), &pb.DeviceRequest{Ski: dumpSKI})
	if err != nil {
		t.Fatal(err)
	}

	if dump.RedactedSki != eebus.ShortSKI(dumpSKI) || !dump.CapturedAt.AsTime().Equal(capturedAt) {
		t.Fatalf("dump header = %s / %s", dump.RedactedSki, dump.CapturedAt.AsTime())
	}
	var decoded eebus.DeviceStructure
	if err := json.Unmarshal([]byte(dump.Json), &decoded); err != nil {
		t.Fatalf("dump json does not decode: %v", err)
	}
	if len(decoded.Entities) != 1 || decoded.Entities[0].Address != "2:1" {
		t.Fatalf("decoded entities = %+v", decoded.Entities)
	}
	if !slices.Equal(dumps.calls, []string{dumpSKI}) {
		t.Fatalf("dump calls = %v", dumps.calls)
	}
}

func TestGetDeviceStructureDumpErrors(t *testing.T) {
	notConnected := &fakeStructureDumps{err: eebus.ErrRemoteDeviceNotConnected}
	broken := &fakeStructureDumps{err: errors.New("boom")}
	tests := []struct {
		name  string
		dumps bridgegrpc.StructureDumpSource
		ski   string
		want  codes.Code
	}{
		{name: "missing ski", dumps: notConnected, ski: "", want: codes.InvalidArgument},
		{name: "malformed ski", dumps: notConnected, ski: "1234", want: codes.InvalidArgument},
		{name: "unknown device", dumps: notConnected, ski: "89ABCDEF0123456789ABCDEF0123456789ABCDEF", want: codes.NotFound},
		{name: "not connected", dumps: notConnected, ski: dumpSKI, want: codes.Unavailable},
		{name: "dump failed", dumps: broken, ski: dumpSKI, want: codes.Internal},
		{name: "dumps not wired", dumps: nil, ski: dumpSKI, want: codes.Unimplemented},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newDumpService(t, tt.dumps).GetDeviceStructureDump(context.Background(), &pb.DeviceRequest{Ski: tt.ski})
			if got := status.Code(err); got != tt.want {
				t.Fatalf("code = %s, want %s (err %v)", got, tt.want, err)
			}
		})
	}
}

func TestServerInfoAdvertisesStructureDump(t *testing.T) {
	info, err := newDumpService(t, &fakeStructureDumps{}).GetServerInfo(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(info.Features, pb.FeatureId_FEATURE_DEVICE_STRUCTURE_DUMP) {
		t.Fatalf("features = %v, want FEATURE_DEVICE_STRUCTURE_DUMP", info.Features)
	}
}
