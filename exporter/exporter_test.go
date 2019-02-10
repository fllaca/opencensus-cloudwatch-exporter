package exporter

import (
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"testing"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

type mockedReceiveMsgs struct {
	cloudwatchiface.CloudWatchAPI
	requests []*cloudwatch.PutMetricDataInput
}

func (m *mockedReceiveMsgs) PutMetricData(request *cloudwatch.PutMetricDataInput) (*cloudwatch.PutMetricDataOutput, error) {
	m.requests = append(m.requests, request)
	return nil, nil
}

var (
	KeyTest, _  = tag.NewKey("test")
)

func TestQueueGetMessage(t *testing.T) {
	count1 := &view.CountData{Value: 1}
	last1 := &view.LastValueData{Value: 1}
	sum1 := &view.SumData{Value: 1}
	cases := []struct {
		ViewData     *view.Data
		Expected []*cloudwatch.PutMetricDataInput
	}{
		// Case Count
		{
			ViewData: &view.Data{
				View: &view.View{
					Name: "foo",
				},
				Rows: []*view.Row{
					{
						Data: count1,
						Tags: []tag.Tag{
							{ Key: KeyTest, Value: "testvalue"},
						},
					},
				},
			},
			Expected: []*cloudwatch.PutMetricDataInput{
				{
					Namespace: aws.String("bar"), 
					MetricData: []*cloudwatch.MetricDatum{
						{
							MetricName: aws.String("foo"),
							Value: aws.Float64(1),
							Unit: aws.String("None"),
							Dimensions: []*cloudwatch.Dimension{
								{ Name: aws.String("test"), Value: aws.String("testvalue") },
							},
						},
					},
				},
			},
		},
		// Case LastValue
		{
			ViewData: &view.Data{
				View: &view.View{
					Name: "foo",
				},
				Rows: []*view.Row{
					{
						Data: last1,
						Tags: []tag.Tag{
							{ Key: KeyTest, Value: "testvalue"},
						},
					},
				},
			},
			Expected: []*cloudwatch.PutMetricDataInput{
				{
					Namespace: aws.String("bar"), 
					MetricData: []*cloudwatch.MetricDatum{
						{
							MetricName: aws.String("foo"),
							Value: aws.Float64(1),
							Unit: aws.String("None"),
							Dimensions: []*cloudwatch.Dimension{
								{ Name: aws.String("test"), Value: aws.String("testvalue") },
							},
						},
					},
				},
			},
		},
		// Case Sum
		{
			ViewData: &view.Data{
				View: &view.View{
					Name: "foo",
				},
				Rows: []*view.Row{
					{
						Data: sum1,
						Tags: []tag.Tag{
							{ Key: KeyTest, Value: "testvalue"},
						},
					},
				},
			},
			Expected: []*cloudwatch.PutMetricDataInput{
				{
					Namespace: aws.String("bar"), 
					MetricData: []*cloudwatch.MetricDatum{
						{
							MetricName: aws.String("foo"),
							Value: aws.Float64(1),
							Unit: aws.String("None"),
							Dimensions: []*cloudwatch.Dimension{
								{ Name: aws.String("test"), Value: aws.String("testvalue") },
							},
						},
					},
				},
			},
		},
	}

	for _, c := range cases {
		cwe := cloudWatchExporter {
			opts: Options{ Namespace: "bar" },
			cwapi: &mockedReceiveMsgs{
				requests: []*cloudwatch.PutMetricDataInput{},
			},
		}
		cwe.ExportView(c.ViewData)
		sentRequests := cwe.cwapi.(*mockedReceiveMsgs).requests
		if ! reflect.DeepEqual(sentRequests, c.Expected) {
			t.Fatalf("Sent request is not the same as expeted")
		}
	}
}