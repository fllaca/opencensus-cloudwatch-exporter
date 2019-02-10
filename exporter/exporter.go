package exporter

import (
	"log"
	"fmt"
	"regexp"

	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

type cloudWatchExporter struct{
	opts Options
    cwapi cloudwatchiface.CloudWatchAPI
}

// Options contains configuration of CloudWatch Exporter
type Options struct {
	// Namespace for CloudWatch metrics
	Namespace string
	// OnError function to be executed if an error occurs while exporting to CloudWatch
	OnError   func(error)
	// UseSharedAwsConfig Initialize a session loading
    // credentials from the shared credentials file ~/.aws/credentials
    // and configuration from the shared configuration file ~/.aws/config.
	UseSharedAwsConfig bool

	// UseEnvCredentials Initialize a session loading credentials from 
	// the AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY env variables 
	UseEnvCredentials bool

	// Region must be specified when using "UseEnvCredentials"
	Region string
}

func (ce *cloudWatchExporter) ExportView(vd *view.Data) {

	err := ce.putMetricsFromView(vd)
	
	if err != nil && ce.opts.OnError != nil {
		ce.opts.OnError(err)
	}
}

// New creates a new CloudWatch Exporter
func New(opts Options) (view.Exporter, error) {
	exporter := new(cloudWatchExporter)

	sessionOpts := session.Options{}

	if opts.UseSharedAwsConfig {
		sessionOpts.SharedConfigState = session.SharedConfigEnable
	} else if opts.UseEnvCredentials {
		sessionOpts.Config = aws.Config{
			Region:      aws.String(opts.Region),
			Credentials: credentials.NewEnvCredentials(),
		}
	} else {
		return nil, fmt.Errorf("One of \"UseSharedAwsConfig,UseEnvCredentials\" must be specified")
	}

	sess := session.Must(session.NewSessionWithOptions(sessionOpts))
	
	// Create new cloudwatch client.
	exporter.opts = opts
	exporter.cwapi = cloudwatch.New(sess)

	return exporter, nil
}

func buildMetricData(vd *view.Data) []*cloudwatch.MetricDatum {
	metricData := []*cloudwatch.MetricDatum{}
	
	for _, row := range vd.Rows {
		metricDataRow, _ := buildMetricDataRow(vd.View, row)
		metricData = append(metricData, metricDataRow)
	}

	return metricData
}

func dimensionsFromTags(tags []tag.Tag) []*cloudwatch.Dimension {
	dimensions := []*cloudwatch.Dimension{}
	for _, t := range tags {
		dimension := &cloudwatch.Dimension{
			Name:  aws.String(t.Key.Name()),
			Value: aws.String(t.Value),
		}
		dimensions = append(dimensions, dimension)
	}
	return dimensions
}

func buildMetricDataRow(vd *view.View, row *view.Row) (*cloudwatch.MetricDatum, error) {
	dimensions := dimensionsFromTags(row.Tags)
	metricName := sanitize(vd.Name)

	switch data := row.Data.(type) {
	case *view.CountData:
		return &cloudwatch.MetricDatum{
			MetricName: aws.String(metricName),
			Unit:       aws.String("None"),
			Value:      aws.Float64(float64(data.Value)),
			Dimensions: dimensions,
		}, nil
	case *view.DistributionData:
		// TODO
		return nil, fmt.Errorf("aggregation %T is not yet supported", vd.Aggregation)
	case *view.SumData:
		return &cloudwatch.MetricDatum{
			MetricName: aws.String(metricName),
			Unit:       aws.String("None"),
			Value:      aws.Float64(data.Value),
			Dimensions: dimensions,
		}, nil
	case *view.LastValueData:
		return &cloudwatch.MetricDatum{
			MetricName: aws.String(metricName),
		    Unit:       aws.String("None"),
			Value:      aws.Float64(data.Value),
			Dimensions: dimensions,
		}, nil
	default:
		return nil, fmt.Errorf("aggregation %T is not yet supported", vd.Aggregation)
	}
}

// based on https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/go/example_code/cloudwatch/custom_metrics.go
func (ce *cloudWatchExporter) putMetricsFromView(vd *view.Data) error {
	metricData := buildMetricData(vd)
	_, err := ce.cwapi.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: &ce.opts.Namespace,
		MetricData: metricData,
	})
	
	return err
}

// TODO use something lighter than regexp evals
func sanitize(text string) string {
    // Make a Regex to say we only want letters and numbers
    reg, err := regexp.Compile("[^a-zA-Z0-9]+")
    if err != nil {
        log.Fatal(err)
    }
    return reg.ReplaceAllString(text, "_")
}