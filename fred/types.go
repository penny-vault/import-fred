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

type Eod struct {
	Date          string  `json:"date" parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Ticker        string  `json:"ticker" parquet:"name=ticker, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Exchange      string  `json:"exchange" parquet:"name=exchange, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AssetType     string  `json:"assetType" parquet:"name=assetType, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CompositeFigi string  `json:"compositeFigi" parquet:"name=compositeFigi, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Open          float32 `json:"open" parquet:"name=open, type=FLOAT"`
	High          float32 `json:"high" parquet:"name=high, type=FLOAT"`
	Low           float32 `json:"low" parquet:"name=low, type=FLOAT"`
	Close         float32 `json:"close" parquet:"name=close, type=FLOAT"`
	Volume        int64   `json:"volume" parquet:"name=volume, type=INT64, convertedtype=INT_64"`
	Dividend      float32 `json:"divCash" parquet:"name=dividend, type=FLOAT"`
	Split         float32 `json:"splitFactor" parquet:"name=split, type=FLOAT"`
}

type Asset struct {
	CompositeFigi string `json:"compositeFigi"`
	Ticker        string `json:"ticker" csv:"ticker"`
	AssetType     string `json:"assetType" csv:"assetType"`
}
