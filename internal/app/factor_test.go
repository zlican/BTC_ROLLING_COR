package app

import (
	"math"
	"testing"
	"time"
)

func TestComputeReturnSeries(t *testing.T) {
	got := computeReturnSeries([]float64{100, 110, 99, 0, 10})
	want := []float64{0.1, -0.1, -1, 0}

	assertFloatSliceClose(t, got, want)
}

func TestAlignPairSeriesSortsOverlappingTimestamps(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	assetSeries := []dataPoint{
		{Time: base.Add(2 * time.Hour), Close: 120},
		{Time: base, Close: 100},
		{Time: base.Add(time.Hour), Close: 110},
	}
	benchmarkSeries := []dataPoint{
		{Time: base.Add(time.Hour), Close: 210},
		{Time: base.Add(3 * time.Hour), Close: 230},
		{Time: base, Close: 200},
	}

	dates, assetValues, benchmarkValues, err := alignPairSeries(assetSeries, benchmarkSeries)
	if err != nil {
		t.Fatalf("alignPairSeries returned error: %v", err)
	}

	wantDates := []time.Time{base, base.Add(time.Hour)}
	if len(dates) != len(wantDates) {
		t.Fatalf("dates length = %d, want %d", len(dates), len(wantDates))
	}
	for i := range wantDates {
		if !dates[i].Equal(wantDates[i]) {
			t.Fatalf("dates[%d] = %s, want %s", i, dates[i], wantDates[i])
		}
	}
	assertFloatSliceClose(t, assetValues, []float64{100, 110})
	assertFloatSliceClose(t, benchmarkValues, []float64{200, 210})
}

func TestRollingCorrelationAndBeta(t *testing.T) {
	assetReturns := []float64{-2, -1, 0, 1, 2}
	benchmarkReturns := []float64{-1, -0.5, 0, 0.5, 1}

	corrValues, err := rollingCorrelationRaw(assetReturns, benchmarkReturns, 3)
	if err != nil {
		t.Fatalf("rollingCorrelationRaw returned error: %v", err)
	}
	assertFloatSliceClose(t, corrValues, []float64{1, 1, 1})

	dates := []time.Time{
		time.Unix(1, 0).UTC(),
		time.Unix(2, 0).UTC(),
		time.Unix(3, 0).UTC(),
		time.Unix(4, 0).UTC(),
		time.Unix(5, 0).UTC(),
	}
	betaPoints, betaValues, err := rollingBetaPoints(dates, assetReturns, benchmarkReturns, 3)
	if err != nil {
		t.Fatalf("rollingBetaPoints returned error: %v", err)
	}
	assertFloatSliceClose(t, betaValues, []float64{2, 2, 2})
	if !betaPoints[0].Time.Equal(dates[2]) {
		t.Fatalf("first beta point time = %s, want %s", betaPoints[0].Time, dates[2])
	}
}

func TestClassifySignalCode(t *testing.T) {
	tests := []struct {
		name string
		corr float64
		beta float64
		want string
	}{
		{name: "independent", corr: 0.749, beta: 3, want: signalIndependent},
		{name: "follow", corr: 0.75, beta: 1.5, want: signalFollow},
		{name: "strong follow", corr: 0.9, beta: 1.51, want: signalStrongFollow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifySignalCode(tt.corr, tt.beta); got != tt.want {
				t.Fatalf("classifySignalCode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func assertFloatSliceClose(t *testing.T, got, want []float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length = %d, want %d; got=%v want=%v", len(got), len(want), got, want)
	}
	for i := range want {
		if math.Abs(got[i]-want[i]) > 1e-9 {
			t.Fatalf("value[%d] = %.12f, want %.12f; got=%v want=%v", i, got[i], want[i], got, want)
		}
	}
}
