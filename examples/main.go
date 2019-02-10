package main

import (
	"log"
	"context"
	"time"

	"math/rand"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.opencensus.io/stats/view"
	cloudwatch "github.com/fllaca/opencensus-cloudwatch-exporter/exporter"
)

var (
	// RandomMetric exposes a random number (in milliseconds, why not?)
	RandomMetric = stats.Float64("example/random", "Random number", "ms")

	// KeySign indicates the random number sign (positive/negative)
	KeySign, _  = tag.NewKey("sign")

	// RandomLastView exposes the last of random numbers
	RandomLastView = &view.View{
		Name:        "example/random_last",
		Measure:     RandomMetric,
		Description: "The last of random numbers",
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{KeySign},
	}
)

func init() {
	if err := view.Register(RandomLastView); err != nil {
		log.Fatalf("Failed to register views: %v", err)
	}
}

func main() {
	ce, err := cloudwatch.New(cloudwatch.Options{
		Namespace: "demo",
		OnError: func(err error){
			log.Fatalf("Failed to push metrics: %v", err)
		},
		UseSharedAwsConfig: true,
	})

	if err != nil {
		log.Fatalf("Cannot create CloudWatch exporter: %v", err)
	}

	view.RegisterExporter(ce)

	for {
		if err := calculateRandom(-100, 100); err != nil {
			log.Fatal(err)
		}

		time.Sleep(2 * time.Second)
	}
}

func calculateRandom(start float64, end float64) error {
	var result float64
	var sign = "positive"

	r := rand.Float64()
	result = ((end - start) * r) + start

	if result < 0 {
		sign = "negative"
	}

	ctx, err := tag.New(context.Background(), tag.Insert(KeySign, sign))
	if err != nil {
		return err
	}
	stats.Record(ctx, RandomMetric.M(result))
	return nil
}