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
	subLog.Info().Msg("checking for missing values")
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, viper.GetString("database.url"))
	if err != nil {
		subLog.Error().Err(err).Msg("Could not connect to database")
		return err
	}
	defer conn.Close(ctx)

	// get since date
	var since time.Time
	if err = conn.QueryRow(ctx, "SELECT event_date FROM eod WHERE composite_figi=$1 ORDER BY event_date ASC LIMIT 1", asset.CompositeFigi).Scan(&since); err != nil {
		subLog.Error().Err(err).Msg("could not retrieve first date")
		return err
	}

	max_age := viper.GetDuration("max_age_forward_fill")
	max_age_dt := time.Now().Add(max_age * -1)
	if max_age_dt.After(since) {
		since = max_age_dt
	}

	subLog.Info().Time("Since", since).Msg("first date for forward-fill")

	// initialize prevValue
	var prevValue float64
	if err = conn.QueryRow(ctx, "SELECT close FROM eod WHERE composite_figi=$1 AND event_date < $2 ORDER BY event_date DESC LIMIT 1", asset.CompositeFigi, since).Scan(&prevValue); err != nil {
		subLog.Error().Err(err).Msg("could not retrieve first value")
		return err
	}

	// create a new transaction for inserts
	tx, err := conn.Begin(ctx)
	if err != nil {
		subLog.Error().Err(err).Msg("could not begin transaction")
		return err
	}

	// remove fill values in the since period (in-case additional values were published by the true source)
	if _, err = tx.Exec(ctx, `DELETE FROM eod WHERE composite_figi = $1 AND event_date >= $2 AND source = 'api.pennyvault.com'`, asset.CompositeFigi, since); err != nil {
		subLog.Error().Err(err).Msg("could not remove old values entered by penny vault")
		check(tx.Rollback(ctx), "transaction rollback failed")
		return err
	}

	// get a list of valid trading days
	tradingDays := make([]time.Time, 0, 252*50)
	rows, err := conn.Query(ctx, "SELECT trading_day FROM trading_days WHERE trading_day >= $1 ORDER BY trading_day ASC", since)
	if err != nil {
		subLog.Error().Err(err).Msg("query database for trading days failed")
		check(tx.Rollback(ctx), "rollback failed")
		return err
	}

	for rows.Next() {
		var dt time.Time
		err = rows.Scan(&dt)
		if err != nil {
			subLog.Error().Err(err).Msg("could not scan date value")
			check(tx.Rollback(ctx), "rollback failed")
			return err
		}
		tradingDays = append(tradingDays, dt)
	}

	for _, dt := range tradingDays {
		// check if missing val
		cnt := 0
		err = conn.QueryRow(ctx, "SELECT count(*) FROM eod WHERE composite_figi=$1 AND event_date=$2", asset.CompositeFigi, dt).Scan(&cnt)
		if err != nil {
			subLog.Error().Err(err).Time("EventDate", dt).Msg("could not determine count for given date")
			check(tx.Rollback(ctx), "transaction rollback failed")
			return err
		}

		if cnt != 1 {
			// value is missing, fill forward
			subLog.Info().Time("EventDate", dt).Float64("PrevValue", prevValue).Msg("missing value in history")
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
				$11
			)`, asset.Ticker, asset.CompositeFigi, dt, prevValue, prevValue, prevValue, prevValue, 0, 0, 1, "api.pennyvault.com"); err != nil {
				subLog.Error().Err(err).Msg("could not insert row into database")
				check(tx.Rollback(ctx), "transaction rollback failed")
				return err
			}
		} else {
			// update forward-fill value
			if err = conn.QueryRow(ctx, "SELECT close FROM eod WHERE composite_figi=$1 AND event_date = $2", asset.CompositeFigi, dt).Scan(&prevValue); err != nil {
				subLog.Error().Err(err).Time("EventDate", dt).Msg("could not update fill-forward value")
				check(tx.Rollback(ctx), "transaction rollback failed")
				return err
			}
		}
	}

	check(tx.Commit(ctx), "transaction commit failed")
	return nil
}
