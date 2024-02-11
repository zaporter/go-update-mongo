package update

import (
	"github.com/pkg/errors"
	"github.com/zaporter-work/go-update-mongo/internal/ferret/bson2"
	"github.com/zaporter-work/go-update-mongo/internal/ferret/handler/common"
	"github.com/zaporter-work/go-update-mongo/internal/ferret/types"
	"go.mongodb.org/mongo-driver/bson"
)

type Document = bson.D
type UpdateOperation = []bson.D
type upT = map[string]any

func UpdateDocument(document Document, updates UpdateOperation) (Document, error) {
	if len(updates) == 0 {
		return nil, errors.New("update document must have at least one element")
	}
	doc, err := convertDToDocument(document)
	if err != nil {
		return nil, err
	}
	convertedUpdates, err := convertUpdateParams(updates)
	if err != nil {
		return nil, errors.Wrap(err, "convert update operations to update params")
	}
	for _, update := range convertedUpdates {
		// from ferret/handler/msg_update.go
		_, err := common.HasSupportedUpdateModifiers("update", update.Update)
		if err != nil {
			return nil, err
		}

		if _, err = common.UpdateDocument("update", doc, update.Update, true); err != nil {
			return nil, errors.Wrap(err, "failed to update document")
		}

		if !doc.Has("_id") {
			doc.Set("_id", types.NewObjectID())
		}
		if err = doc.ValidateData(); err != nil {
			return nil, err
		}
	}
	result, err := convertDocumentToD(doc)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func convertDToDocument(d bson.D) (*types.Document, error) {
	// from ferret/bson2/document_test.go
	bytes, err := bson.Marshal(d)
	if err != nil {
		return nil, errors.Wrap(err, "marshall bson.D")
	}

	rawDoc, err := bson2.RawDocument(bytes).DecodeDeep()
	if err != nil {
		return nil, errors.Wrap(err, "decode raw bson bytes")
	}

	doc, err := rawDoc.Convert()
	// todo: doc.validatedata?
	return doc, errors.Wrap(err, "converting to parsed bson")
}

func convertDocumentToD(document *types.Document) (bson.D, error) {
	bson2Doc, err := bson2.ConvertDocument(document)
	if err != nil {
		return nil, errors.Wrap(err, "convert from types.Document to bson2.Document")
	}
	bytes, err := bson2Doc.Encode()
	if err != nil {
		return nil, errors.Wrap(err, "encode bson2.Document")
	}
	decoded := bson.D{}
	err = bson.Unmarshal(bytes, &decoded)
	if err != nil {
		return nil, errors.Wrap(err, "decode internal bson2 back to bson.D")
	}
	return decoded, nil
}
func convertUpdateParams(updates UpdateOperation) ([]common.Update, error) {
	commonUpdates := make([]common.Update, 0, len(updates))

	for _, update := range updates {
		updateDocument, err := convertDToDocument(update)
		if err != nil {
			return nil, errors.Wrap(err, "convert bson.A update to internal update document")
		}
		commonUpdate := common.Update{
			Filter:       updateDocument,
			Update:       updateDocument,
			Multi:        true,
			Upsert:       true,
			C:            updateDocument,
			Collation:    updateDocument,
			ArrayFilters: nil,
			Hint:         "",
		}
		if err := common.ValidateUpdateOperators("update", commonUpdate.Update); err != nil {
			return nil, err
		}
		commonUpdates = append(commonUpdates, commonUpdate)
	}
	return commonUpdates, nil
}
