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
package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/penny-vault/import-fred/fred"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "import-fred",
	Short: "Download end-of-day quotes from fred",
	Long:  `Download end-of-day quotes from fred and save to penny-vault database`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		assets := fred.LoadAssetsFromDB()

		limit := viper.GetInt("limit")
		if limit > 0 {
			assets = assets[:limit]
		}

		quotes := fred.Fetch(assets)
		if viper.GetString("parquet_file") != "" {
			err := fred.SaveToParquet(quotes, viper.GetString("parquet_file"))
			if err != nil {
				log.Error().Err(err).Msg("failed to save to parquet file")
			}
		}

		if viper.GetString("database.url") != "" {
			err := fred.SaveToDatabase(quotes)
			if err != nil {
				log.Error().Err(err).Msg("failed to save to database")
			}
		}

		for _, asset := range assets {
			err := fred.Fill(asset)
			if err != nil {
				log.Error().Err(err).Msg("failed to fill missing assets")
			}
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)
	cobra.OnInitialize(initLog)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is import-fred.toml)")
	rootCmd.PersistentFlags().Bool("log.json", false, "print logs as json to stderr")
	err := viper.BindPFlag("log.json", rootCmd.PersistentFlags().Lookup("log.json"))
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind pflag for log.json")
	}

	// Local flags
	rootCmd.Flags().StringP("database-url", "d", "host=localhost port=5432", "DSN for database connection")
	err = viper.BindPFlag("database.url", rootCmd.Flags().Lookup("database-url"))
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind pflag for database.url")
	}

	rootCmd.Flags().Uint32P("limit", "l", 0, "limit results to N")
	err = viper.BindPFlag("limit", rootCmd.Flags().Lookup("limit"))
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind pflag for limit")
	}

	rootCmd.Flags().Int("fred-rate-limit", 5, "fred rate limit (items per second)")
	err = viper.BindPFlag("fred_rate_limit", rootCmd.Flags().Lookup("fred-rate-limit"))
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind pflag for fred_rate_limit")
	}

	rootCmd.Flags().Duration("max-age-forward-fill", time.Duration(time.Hour*24*90), "maximum age of eod values to calculate forwrad fill for")
	err = viper.BindPFlag("max_age_forward_fill", rootCmd.Flags().Lookup("max-age-forward-fill"))
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind pflag for max_age_forward_fill")
	}

	rootCmd.Flags().String("parquet-file", "", "save results to parquet")
	err = viper.BindPFlag("parquet_file", rootCmd.Flags().Lookup("parquet-file"))
	if err != nil {
		log.Fatal().Err(err).Msg("could not bind pflag for parquet_file")
	}
}

func initLog() {
	if !viper.GetBool("log.json") {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)

		// Search config in home directory with name ".import-fred" (without extension).
		viper.AddConfigPath("/etc") // path to look for the config file in
		viper.AddConfigPath(fmt.Sprintf("%s/.config", home))
		viper.AddConfigPath(".")
		viper.SetConfigType("toml")
		viper.SetConfigName("import-fred")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Debug().Str("ConfigFile", viper.ConfigFileUsed()).Msg("Loaded config file")
	} else {
		log.Error().Err(err).Msg("error reading config file")
	}
}
