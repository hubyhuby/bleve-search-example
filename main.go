//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"encoding/json"
	_ "expvar"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/search/highlight/highlighter/html"
	"github.com/gorilla/mux"
)

var batchSize = flag.Int("batchSize", 100, "batch size for indexing")
var jsonDir = flag.String("jsonDir", "data/", "json directory")
var indexPath = flag.String("index", "beer-search.bleve", "index path")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write mem profile to file")

const sayhi = "<HTML> <CENTER>BLEVE simple demo. Enjoy;)</CENTER><BR> Term search: <a href=./search/brew>/search/brew </a> <BR>Geo Search box<a href=./geosearch/> /geosearch/ </a> <BR>Conjunction Geo<a href=./geosearch/brew> /geosearch/brew </a> <p><img src=https://tooap.com/wp-content/uploads/2017/12/tooap-agence-digitale-logo.png /><BR><BR> Brought to you by <a href=https://tooap.com/> Tooap</a> Digital Innovation  Agency. </HTML></p>"

var beerIndex bleve.Index

func main() {

	// create a router
	r := mux.NewRouter()
	r.HandleFunc("/search/{term}", Search)
	r.HandleFunc("/geosearch/", GeoSearch)
	r.HandleFunc("/geosearch/{term}", GeoSearchConjunction)
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, sayhi)
	})
	fmt.Println("Listening on 127.0.0.1:3000")
	http.ListenAndServe(":3000", r)

}
//GO launchs by default the init function at the launch of your app
func init() {

	flag.Parse()

	log.Printf("GOMAXPROCS: %d", runtime.GOMAXPROCS(-1))

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
	}

	// open the index
	var err error
	beerIndex, err = bleve.Open(*indexPath)
	if err == bleve.ErrorIndexPathDoesNotExist {
		log.Printf("Creating new index...")
		// create a mapping
		indexMapping, err := buildIndexMapping()
		if err != nil {
			log.Fatal(err)
		}

		// old index
	    //	beerIndex, err = bleve.New(*indexPath, indexMapping)
		//Scorch New index (2019): 10X smaller index size
		beerIndex, err = bleve.NewUsing(*indexPath, indexMapping, "scorch", "scorch", nil)

		if err != nil {
			log.Fatal(err)
		}

		// index data in the background
		go func() {
			err = indexBeer(beerIndex)
			if err != nil {
				log.Fatal(err)
			}
			pprof.StopCPUProfile()
			if *memprofile != "" {
				f, err := os.Create(*memprofile)
				if err != nil {
					log.Fatal(err)
				}
				pprof.WriteHeapProfile(f)
				f.Close()
			}
		}()
	} else if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Opening existing index...")
	}

}

//Geo Search box / find records around a location
func GeoSearch(w http.ResponseWriter, r *http.Request) {

	lon, lat := -122.107799, 37.399285

	//distance query
	distanceQuery := bleve.NewGeoDistanceQuery(lon, lat, "100000mi")
	distanceQuery.SetField("geo")

	//execute request on index
	searchRequest := bleve.NewSearchRequest(distanceQuery)
	searchRequest.Explain = true
	searchResults, err := beerIndex.Search(searchRequest)

	if err != nil {
		return
	}
	//searchResults.
	fmt.Println("Search for geo point")

	fmt.Fprintf(w, "\n\nSearch for records around a geo point:\n\n %s ", searchResults)
}

// Geo Ordered
func GeoSearchSort(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	term := vars["term"]
	//lon, lat := -122.107799, 37.399285
	//Search the index
	search := bleve.NewQueryStringQuery(term)
	req := bleve.NewSearchRequest(search)
	req.Size = 30
	req.Highlight = bleve.NewHighlightWithStyle(html.Name)

	/*
		sortGeo, _ := search.NewSortGeoDistance("location", "km", lon, lat, true)

			req.SortByCustom(search.SortOrder{sortGeo})
	*/
	searchResults, err := beerIndex.Search(req)

	if err != nil {
		return
	}
	//searchResults.
	fmt.Println("Search for", term)

	fmt.Fprintf(w, "\n\nResults for: %s \n\n%s ", term, searchResults)
}

//Conjunction Geo
func GeoSearchConjunction(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	term := vars["term"]

	lon, lat := -122.107799, 37.399285
	//Search the index with GEO //https://github.com/blevesearch/bleve/issues/836
	//https://github.com/blevesearch/bleve/issues/599
	//term query
	search := bleve.NewQueryStringQuery(term)
	req := bleve.NewSearchRequest(search)
	req.Size = 30
	req.Highlight = bleve.NewHighlightWithStyle(html.Name)

	//distance query
	distanceQuery := bleve.NewGeoDistanceQuery(lon, lat, "100mi")
	distanceQuery.SetField("geo")

	//Conjonction of the term and distance queries
	conRequest := bleve.NewConjunctionQuery()
	conRequest.AddQuery(search)
	conRequest.AddQuery(distanceQuery)

	//execute request on index
	searchRequest := bleve.NewSearchRequest(conRequest)
	searchResults, err := beerIndex.Search(searchRequest)

	if err != nil {
		return
	}
	//searchResults.
	fmt.Println("Search for", term)

	fmt.Fprintf(w, "\n\nResults for: %s \n\n%s ", term, searchResults)
}

// Term Search with faceting
func Search(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	term := vars["term"]

	//Search the index
	search := bleve.NewQueryStringQuery(term)
	req := bleve.NewSearchRequest(search)
	req.Size = 30
	//req.Highlight = bleve.NewHighlightWithStyle("html")
	req.Highlight = bleve.NewHighlightWithStyle(html.Name)

	stylesFacet := bleve.NewFacetRequest("state", 3) // https://github.com/blevesearch/bleve/blob/master/examples_test.go
	req.AddFacet("styles", stylesFacet)              //https://blevesearch.com/docs/Result-Faceting/

	searchResults, err := beerIndex.Search(req)

	if err != nil {
		return
	}
	fmt.Println("Search for", term)

	//searchResults.

	if searchResults.Facets["styles"].Total > 0 {
		fmt.Fprintf(w, "Facets:\n")
		fmt.Fprintf(w, "%s (%v) \n", searchResults.Facets["styles"].Terms[0].Term, searchResults.Facets["styles"].Terms[0].Count)
		//fmt.Fprintf(w, "%s (%v) \n", searchResults.Facets["styles"].Terms[1].Term, searchResults.Facets["styles"].Terms[1].Count)
	}

	fmt.Fprintf(w, "\n\nResults for: %s \n\n%s ", term, searchResults)

}

//Indexing the JSON datas in the data folder
func indexBeer(i bleve.Index) error {

	// open the directory
	dirEntries, err := ioutil.ReadDir(*jsonDir)
	if err != nil {
		return err
	}

	// walk the directory entries for indexing
	log.Printf("Indexing...")
	count := 0

	startTime := time.Now()
	batch := i.NewBatch()
	batchCount := 0
	for _, dirEntry := range dirEntries {
		filename := dirEntry.Name()
		// read the bytes
		jsonBytes, err := ioutil.ReadFile(*jsonDir + "/" + filename)
		if err != nil {
			return err
		}
		// parse bytes as json
		var jsonDoc interface{}
		err = json.Unmarshal(jsonBytes, &jsonDoc)
		if err != nil {
			return err
		}
		ext := filepath.Ext(filename)
		docID := filename[:(len(filename) - len(ext))]
		batch.Index(docID, jsonDoc)
		batchCount++

		if batchCount >= *batchSize {
			err = i.Batch(batch)
			if err != nil {
				return err
			}
			batch = i.NewBatch()
			batchCount = 0
		}
		count++
		if count%1000 == 0 {
			indexDuration := time.Since(startTime)
			indexDurationSeconds := float64(indexDuration) / float64(time.Second)
			timePerDoc := float64(indexDuration) / float64(count)
			log.Printf("Indexed %d documents, in %.2fs (average %.2fms/doc)", count, indexDurationSeconds, timePerDoc/float64(time.Millisecond))
		}
	}
	// flush the last batch
	if batchCount > 0 {
		err = i.Batch(batch)
		if err != nil {
			log.Fatal(err)
		}
	}
	indexDuration := time.Since(startTime)
	indexDurationSeconds := float64(indexDuration) / float64(time.Second)
	timePerDoc := float64(indexDuration) / float64(count)
	log.Printf("Indexed %d documents, in %.2fs (average %.2fms/doc)", count, indexDurationSeconds, timePerDoc/float64(time.Millisecond))
	return nil
}
