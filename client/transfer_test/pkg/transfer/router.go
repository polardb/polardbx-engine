package transfer

import "fmt"

type pointRouter func(int) string
type scanRouter func() []string

func RouteScan(conf *Config) scanRouter {
	if !conf.ForXDB || conf.Paritions == 0 {
		return func() []string {
			return []string{tablePrefix}
		}
	} else {
		var tables []string
		for i := 0; i < conf.Paritions; i++ {
			tables = append(tables, fmt.Sprintf("%s_%06d", tablePrefix, i))
		}
		return func() []string {
			return tables
		}
	}
}

func RoutePoint(conf *Config) pointRouter {
	if !conf.ForXDB || conf.Paritions == 0 {
		return func(int) string {
			return tablePrefix
		}
	} else {
		return func(id int) string {
			return fmt.Sprintf("%s_%06d", tablePrefix, id%conf.Paritions)
		}
	}
}
