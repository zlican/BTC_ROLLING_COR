package app

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

func buildFactorFrame(timeframe string, dates []time.Time, assetReturns, benchmarkReturns []float64, window int) (*FactorFrame, error) {
	if variance(assetReturns) < minReturnVar || variance(benchmarkReturns) < minReturnVar {
		return nil, errors.New("return variance is too small for stable factor calculation")
	}

	corrPoints, err := rollingCorrelationPoints(dates, assetReturns, benchmarkReturns, window)
	if err != nil {
		return nil, err
	}

	betaPoints, _, err := rollingBetaPoints(dates, assetReturns, benchmarkReturns, window)
	if err != nil {
		return nil, err
	}

	latestCorr := corrPoints[len(corrPoints)-1]
	latestBeta := betaPoints[len(betaPoints)-1]

	return &FactorFrame{
		Timeframe:  timeframe,
		Status:     statusOK,
		SignalCode: classifySignalCode(latestCorr.Value, latestBeta.Value),
		LatestTime: latestCorr.Time,
		LatestCorr: latestCorr.Value,
		LatestBeta: latestBeta.Value,
		CorrPoints: corrPoints,
		BetaPoints: betaPoints,
	}, nil
}

func classifySignalCode(corr, beta float64) string {
	if corr < 0.75 {
		return signalIndependent
	}
	if beta > 1.5 {
		return signalStrongFollow
	}
	return signalFollow
}

func alignPairSeries(assetSeries, benchmarkSeries []dataPoint) ([]time.Time, []float64, []float64, error) {
	if len(assetSeries) == 0 || len(benchmarkSeries) == 0 {
		return nil, nil, nil, errors.New("one side of pair series is empty")
	}

	assetMap := make(map[time.Time]float64, len(assetSeries))
	for _, point := range assetSeries {
		assetMap[point.Time] = point.Close
	}

	var dates []time.Time
	for _, point := range benchmarkSeries {
		if _, ok := assetMap[point.Time]; ok {
			dates = append(dates, point.Time)
		}
	}
	if len(dates) == 0 {
		return nil, nil, nil, errors.New("no overlapping timestamps between asset and benchmark")
	}

	sort.Slice(dates, func(i, j int) bool {
		return dates[i].Before(dates[j])
	})

	benchmarkMap := make(map[time.Time]float64, len(benchmarkSeries))
	for _, point := range benchmarkSeries {
		benchmarkMap[point.Time] = point.Close
	}

	assetValues := make([]float64, 0, len(dates))
	benchmarkValues := make([]float64, 0, len(dates))
	for _, ts := range dates {
		assetValues = append(assetValues, assetMap[ts])
		benchmarkValues = append(benchmarkValues, benchmarkMap[ts])
	}

	return dates, assetValues, benchmarkValues, nil
}

func computeReturns(dates []time.Time, assetPrices, benchmarkPrices []float64) ([]time.Time, []float64, []float64, error) {
	if len(dates) != len(assetPrices) || len(assetPrices) != len(benchmarkPrices) {
		return nil, nil, nil, errors.New("price series length mismatch")
	}
	if len(dates) < 2 {
		return nil, nil, nil, errors.New("at least two price points are required to compute returns")
	}

	returnDates := append([]time.Time(nil), dates[1:]...)
	assetReturns := computeReturnSeries(assetPrices)
	benchmarkReturns := computeReturnSeries(benchmarkPrices)
	return returnDates, assetReturns, benchmarkReturns, nil
}

func computeReturnSeries(prices []float64) []float64 {
	returns := make([]float64, 0, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		if prices[i-1] == 0 {
			returns = append(returns, 0)
			continue
		}
		returns = append(returns, (prices[i]-prices[i-1])/prices[i-1])
	}
	return returns
}

func rollingCorrelationPoints(dates []time.Time, seriesA, seriesB []float64, window int) ([]FactorPoint, error) {
	values, err := rollingCorrelationRaw(seriesA, seriesB, window)
	if err != nil {
		return nil, err
	}
	return valuesToPoints(dates, values, window), nil
}

func rollingCorrelationRaw(seriesA, seriesB []float64, window int) ([]float64, error) {
	if len(seriesA) != len(seriesB) {
		return nil, errors.New("series length mismatch")
	}
	if len(seriesA) < window {
		return nil, fmt.Errorf("series length %d is smaller than rolling window %d", len(seriesA), window)
	}

	values := make([]float64, 0, len(seriesA)-window+1)
	for end := window; end <= len(seriesA); end++ {
		start := end - window
		values = append(values, correlation(seriesA[start:end], seriesB[start:end]))
	}
	return values, nil
}

func rollingBetaPoints(dates []time.Time, assetReturns, benchmarkReturns []float64, window int) ([]FactorPoint, []float64, error) {
	if len(assetReturns) != len(benchmarkReturns) {
		return nil, nil, errors.New("series length mismatch")
	}
	if len(assetReturns) < window {
		return nil, nil, fmt.Errorf("series length %d is smaller than rolling window %d", len(assetReturns), window)
	}

	values := make([]float64, 0, len(assetReturns)-window+1)
	for end := window; end <= len(assetReturns); end++ {
		start := end - window
		cov := covariance(assetReturns[start:end], benchmarkReturns[start:end])
		varBenchmark := variance(benchmarkReturns[start:end])
		if varBenchmark == 0 {
			values = append(values, 0)
			continue
		}
		values = append(values, cov/varBenchmark)
	}

	return valuesToPoints(dates, values, window), values, nil
}

func valuesToPoints(dates []time.Time, values []float64, window int) []FactorPoint {
	points := make([]FactorPoint, 0, len(values))
	for i, value := range values {
		dateIndex := i + window - 1
		points = append(points, FactorPoint{
			Time:  dates[dateIndex],
			Value: value,
		})
	}
	return points
}

func correlation(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var sumA float64
	var sumB float64
	for i := range a {
		sumA += a[i]
		sumB += b[i]
	}

	meanA := sumA / float64(len(a))
	meanB := sumB / float64(len(b))

	var covarianceValue float64
	var varianceA float64
	var varianceB float64
	for i := range a {
		deltaA := a[i] - meanA
		deltaB := b[i] - meanB
		covarianceValue += deltaA * deltaB
		varianceA += deltaA * deltaA
		varianceB += deltaB * deltaB
	}

	if varianceA == 0 || varianceB == 0 {
		return 0
	}

	value := covarianceValue / math.Sqrt(varianceA*varianceB)
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func covariance(a, b []float64) float64 {
	if len(a) != len(b) || len(a) == 0 {
		return 0
	}

	var sumA float64
	var sumB float64
	for i := range a {
		sumA += a[i]
		sumB += b[i]
	}

	meanA := sumA / float64(len(a))
	meanB := sumB / float64(len(b))

	var value float64
	for i := range a {
		value += (a[i] - meanA) * (b[i] - meanB)
	}
	return value / float64(len(a))
}

func variance(series []float64) float64 {
	if len(series) == 0 {
		return 0
	}

	var sum float64
	for _, value := range series {
		sum += value
	}
	mean := sum / float64(len(series))

	var acc float64
	for _, value := range series {
		delta := value - mean
		acc += delta * delta
	}
	return acc / float64(len(series))
}
