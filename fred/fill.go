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
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"
)

// Fill checks that all trading days have a value for the given
// FRED ticker. If a point is missing the previous point is propgated
// forward.
func Fill(asset *Asset) error {
	subLog := log.With().Str("figi", asset.CompositeFigi).Str("ticker", asset.Ticker).Logger()
	subLog.Info().Str("Figi", asset.CompositeFigi).Msg("checking for missing values")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		subLog.Error().Err(err).Msg("Could not connect to database")
		return err
	}
	defer conn.Close(ctx)

	// initialize first value that is used for forward-fill
	var prevValue float64
	var since time.Time
	if err = conn.QueryRow(ctx, "SELECT event_date, close FROM trading_day WHERE composite_figi=$1 ORDER BY event_date ASC LIMIT 1", asset.CompositeFigi).Scan(&since, &prevValue); err != nil {
		subLog.Error().Err(err).Msg("could not retrieve first value")
		return err
	}

	// create a new transaction for inserts
	tx, err := conn.Begin(ctx)
	if err != nil {
		subLog.Error().Err(err).Msg("could not begin transaction")
		return err
	}

	// get a list of valid trading days
	rows, err := conn.Query(ctx, "SELECT trading_day FROM trading_days WHERE >= $1", since)
	if err != nil {
		subLog.Error().Err(err).Msg("query database for trading days failed")
		tx.Rollback(ctx)
		return err
	}
	for rows.Next() {
		var dt time.Time
		err = rows.Scan(&dt)
		if err != nil {
			subLog.Error().Err(err).Msg("could not scan date value")
			tx.Rollback(ctx)
			return err
		}
		// check if missing val
		cnt := 0
		err = conn.QueryRow(ctx, "SELECT count(*) FROM eod WHERE figi=$1 AND event_date=$2", asset.CompositeFigi, dt).Scan(&cnt)
		if err != nil {
			subLog.Error().Err(err).Time("EventDate", dt).Msg("could not determine count for given date")
			tx.Rollback(ctx)
			return err
		}

		if cnt != 1 {
			// value is missing, fill forward
			if _, err = tx.Exec(ctx, `INSERT INTO eod (
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
				$11`, asset.Ticker, asset.CompositeFigi, dt, prevValue, prevValue, prevValue, prevValue, 0, 0, 1, "fred.stlouisfed.org"); err != nil {
				subLog.Error().Err(err).Msg("could not insert row into database")
				tx.Rollback(ctx)
				return err
			}
		} else {
			// update forward-fill value
			if err = conn.QueryRow(ctx, "SELECT close FROM eod WHERE composite_figi=$1 AND event_date = $2", asset.CompositeFigi, dt).Scan(&prevValue); err != nil {
				subLog.Error().Err(err).Time("EventDate", dt).Msg("could not update fill-forward value")
				tx.Rollback(ctx)
				return err
			}
		}
	}

	tx.Commit(ctx)
	return nil
}
