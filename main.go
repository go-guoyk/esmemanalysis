package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/olivere/elastic/v7"
	"log"
	"os"
	"sort"
)

func main() {
	var err error
	defer func(err *error) {
		if *err != nil {
			log.Println("exited with error:", (*err).Error())
			os.Exit(1)
		} else {
			log.Println("exited")
		}
	}(&err)

	log.SetOutput(os.Stdout)

	var optURL string
	flag.StringVar(&optURL, "url", "http://127.0.0.1:9200", "url of elasticsearch")
	flag.Parse()

	var client *elastic.Client
	if client, err = elastic.NewClient(
		elastic.SetURL(optURL),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	); err != nil {
		return
	}

	var stats *elastic.IndicesStatsResponse
	if stats, err = client.IndexStats().Do(context.Background()); err != nil {
		return
	}

	log.Printf("ALL: %s", indexStatsSummary(stats.All.Total))

	type Item struct {
		Index string
		Stats *elastic.IndexStatsDetails
	}

	var items []Item

	for k, v := range stats.Indices {
		items = append(items, Item{
			Index: k,
			Stats: v.Total,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return indexStatsTotalMemory(items[j].Stats) < indexStatsTotalMemory(items[i].Stats)
	})

	if len(items) > 10 {
		items = items[0:10]
	}

	for _, item := range items {
		log.Printf("%s: %s", item.Index, indexStatsSummary(item.Stats))
	}

}

func indexStatsTotalMemory(stats *elastic.IndexStatsDetails) int64 {
	return stats.QueryCache.MemorySizeInBytes +
		stats.Fielddata.MemorySizeInBytes +
		stats.Segments.MemoryInBytes +
		stats.RequestCache.MemorySizeInBytes
}

func indexStatsSummary(stats *elastic.IndexStatsDetails) string {
	return fmt.Sprintf(
		"QueryCache: %dM, Fielddata: %dM, Segments: %dM, RequestCache: %dM",
		stats.QueryCache.MemorySizeInBytes/1000000,
		stats.Fielddata.MemorySizeInBytes/1000000,
		stats.Segments.MemoryInBytes/1000000,
		stats.RequestCache.MemorySizeInBytes/1000000,
	)
}
