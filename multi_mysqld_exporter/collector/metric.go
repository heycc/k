package collector

type Metric struct {
	Namespace string
	Key       string
	Value     interface{}
}

func NewMetric(namespace string, key string, value interface{}) Metric {
	return Metric{
		namespace,
		key,
		value,
	}
}
