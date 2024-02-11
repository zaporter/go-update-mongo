// Copyright 2021 FerretDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutil

import (
	"os"
	"testing"

	"github.com/zaporter-work/go-update-mongo/internal/ferret/util/testutil/testtb"
)

// TestHanaURI returns a HANA Database URL for testing.
// HANATODO Create a Database per test run?
func TestHanaURI(tb testtb.TB) string {
	tb.Helper()

	if testing.Short() {
		tb.Skip("skipping in -short mode")
	}

	url := os.Getenv("FERRETDB_HANA_URL")
	if url == "" {
		tb.Skip("FERRETDB_HANA_URL is not set")
	}

	return url
}