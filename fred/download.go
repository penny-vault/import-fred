/*
Copyright 2022

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package fred

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
	"go.uber.org/ratelimit"
)

func SaveToParquet(records []*Eod, fn string) error {
	var err error

	fh, err := local.NewLocalFileWriter(fn)
	if err != nil {
		log.Error().Err(err).Str("FileName", fn).Msg("cannot create local file")
		return err
	}
	defer fh.Close()

	pw, err := writer.NewParquetWriter(fh, new(Eod), 4)
	if err != nil {
		log.Error().
			Str("OriginalError", err.Error()).
			Msg("Parquet write failed")
		return err
	}

	pw.RowGroupSize = 128 * 1024 * 1024 // 128M
	pw.PageSize = 8 * 1024              // 8k
	pw.CompressionType = parquet.CompressionCodec_GZIP

	for _, r := range records {
		if err = pw.Write(r); err != nil {
			log.Error().
				Str("OriginalError", err.Error()).
				Str("EventDate", r.Date).Str("Ticker", r.Ticker).
				Str("CompositeFigi", r.CompositeFigi).
				Msg("Parquet write failed for record")
		}
	}

	if err = pw.WriteStop(); err != nil {
		log.Error().Err(err).Msg("Parquet write failed")
		return err
	}

	log.Info().Int("NumRecords", len(records)).Msg("Parquet write finished")
	return nil
}

func Fetch(assets []*Asset) []*Eod {
	// fred rate limits
	limit := ratelimit.New(viper.GetInt("fred_rate_limit"))

	quotes := []*Eod{}
	client := resty.New()
	startDate := time.Now().Add(-7 * 24 * time.Hour)
	startDateStr := startDate.Format("2006-01-02")
	today := time.Now()
	todayStr := today.Format("2006-01-02")

	bar := progressbar.Default(int64(len(assets)))
	for _, asset := range assets {
		bar.Add(1)
		limit.Take()
		url := fmt.Sprintf("https://fred.stlouisfed.org/graph/fredgraph.csv?mode=fred&id=%s&cosd=%s&coed=%s&fq=Daily&fam=avg", asset.Ticker, startDateStr, todayStr)
		log.Debug().Str("Url", url).Msg("Loading URL")
		resp, err := client.
			R().
			SetHeader("Accept", "application/csv").
			Get(url)
		if err != nil {
			log.Error().Err(err).Str("Url", url).Msg("error when requesting eod quote")
			continue
		}
		if resp.StatusCode() >= 400 {
			log.Error().Int("StatusCode", resp.StatusCode()).Str("Url", url).Bytes("Body", resp.Body()).Msg("error when requesting eod quote")
			continue
		}
		data := string(resp.Body())
		lines := strings.Split(data, "\n")
		for _, ll := range lines[1:] {
			parts := strings.Split(ll, ",")
			if len(parts) == 2 {
				if parts[1] == "." {
					continue
				}
				val, err := strconv.ParseFloat(parts[1], 32)
				if err != nil {
					log.Warn().Str("Line", ll).Str("Ticker", asset.Ticker).Str("Val", parts[1]).Err(err).Msg("could not convert str to float")
				}
				val32 := float32(val)
				q := Eod{
					Date:          parts[0],
					Ticker:        asset.Ticker,
					Exchange:      "FRED",
					AssetType:     asset.AssetType,
					CompositeFigi: asset.CompositeFigi,
					Open:          val32,
					High:          val32,
					Low:           val32,
					Close:         val32,
					Split:         1,
				}
				quotes = append(quotes, &q)
			}
		}
	}

	return quotes
}
