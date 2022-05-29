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
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

func LoadAssetsFromDB() (assets []*Asset) {
	assets = make([]*Asset, 0, 5)

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("Could not connect to database")
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `SELECT composite_figi, ticker, asset_type FROM assets WHERE asset_type = 'FRED' AND active = 't'`)
	if err != nil {
		log.Error().Err(err).Msg("could not retrieve FRED assets from the database")
		return
	}

	for rows.Next() {
		var asset Asset
		err = rows.Scan(&asset.CompositeFigi, &asset.Ticker, &asset.AssetType)
		if err != nil {
			log.Error().Err(err).Msg("error scanning row into asset")
		}
		assets = append(assets, &asset)
		log.Info().Str("Ticker", asset.Ticker).Msg("adding asset for download")
	}

	return
}

func SaveToDatabase(quotes []*Eod) error {
	log.Info().Msg("saving to database")
	conn, err := pgx.Connect(context.Background(), viper.GetString("database.url"))
	if err != nil {
		log.Error().Err(err).Msg("Could not connect to database")
	}
	defer conn.Close(context.Background())

	for _, quote := range quotes {
		_, err := conn.Exec(context.Background(),
			`INSERT INTO eod (
			"ticker",
			"composite_figi",
			"event_date",
			"open",
			"high",
			"low",
			"close",
			"volume",
			"dividend",
			"split_factor",
			"source"
		) VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			$6,
			$7,
			$8,
			$9,
			$10,
			$11
		) ON CONFLICT ON CONSTRAINT eod_pkey
		DO UPDATE SET
			open = EXCLUDED.open,
			high = EXCLUDED.high,
			low = EXCLUDED.low,
			close = EXCLUDED.close,
			volume = EXCLUDED.volume,
			dividend = EXCLUDED.dividend,
			split_factor = EXCLUDED.split_factor,
			source = EXCLUDED.source;`,
			quote.Ticker, quote.CompositeFigi, quote.Date,
			quote.Open, quote.High, quote.Low, quote.Close, quote.Volume,
			quote.Dividend, quote.Split, "fred.stlouisfed.org")
		if err != nil {
			query := fmt.Sprintf(`INSERT INTO eod ("ticker", "composite_figi", "event_date", "open", "high", "low", "close", "volume", "dividend", "split_factor", "source") VALUES ('%s', '%s', '%s', %.5f, %.5f, %.5f, %.5f, %d, %.5f, %.5f, '%s') ON CONFLICT ON CONSTRAINT eod_pkey DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume, dividend = EXCLUDED.dividend, split_factor = EXCLUDED.split_factor, source = EXCLUDED.source;`,
				quote.Ticker, quote.CompositeFigi, quote.Date,
				quote.Open, quote.High, quote.Low, quote.Close, quote.Volume,
				quote.Dividend, quote.Split, "fred.stlouisfed.org")
			log.Error().Err(err).Str("Query", query).Msg("error saving EOD quote to database")
		}
	}

	return nil
}
