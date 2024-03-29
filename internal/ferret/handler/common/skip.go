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

package common

import (
	"github.com/zaporter/go-update-mongo/internal/ferret/types"
	"github.com/zaporter/go-update-mongo/internal/ferret/util/lazyerrors"
)

// SkipDocuments returns a subslice of given documents according to the given skip value.
func SkipDocuments(docs []*types.Document, skip int64) ([]*types.Document, error) {
	switch {
	case skip == 0:
		return docs, nil
	case skip > 0:
		if int64(len(docs)) < skip {
			return []*types.Document{}, nil
		}

		return docs[skip:], nil
	default:
		return nil, lazyerrors.Errorf("unexpected skip value: %d", skip)
	}
}
